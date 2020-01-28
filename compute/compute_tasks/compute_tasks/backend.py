import argparse
import json
import logging
import os
import sys
import time
import threading
from functools import partial

import cwt
import jinja2
import zmq

from compute_tasks import base
from compute_tasks import cdat
from compute_tasks import celery_app
from compute_tasks import WPSError
from compute_tasks.job import job_started
from compute_tasks.job import job_succeeded
from compute_tasks.context import state_mixin
from compute_tasks.context import operation

logger = logging.getLogger('compute_tasks.backend')

# Set INGRESS_QUEUE to prevent breaking old code
DEFAULT_QUEUE = {
    'queue': 'ingress',
    'exchange': 'ingress',
    'routing_key': 'ingress',
}

QUEUE = {
    'cdat': {
        'queue': 'ingress',
        'exchange': 'ingress',
        'routing_key': 'ingress',
    },
    'default': {
        'queue': 'default',
        'exchange': 'default',
        'routing_key': 'default',
    },
}

QUEUE.update({'ingress': DEFAULT_QUEUE})

# Handler types
REQUEST_TYPE = 'execute'
RESOURCE_TYPE = 'resource'
ERROR_TYPE = 'error'

# Flags
ACK = b'ACK'
ERR = b'ERR'
HEARTBEAT = b'HEARTBEAT'
READY = b'READY'
REQUEST = b'REQUEST'
RESOURCE = b'RESOURCE'

HEARTBEAT_LIVENESS = 3
HEARTBEAT_INTERVAL = 1.0 * 1000

INTERVAL_INIT = 1
INTERVAL_MAX = 32

TEMPLATE_NAMES = [
    'dask-scheduler-pod.yaml',
    'dask-scheduler-service.yaml',
    'dask-scheduler-ingress.yaml',
    'dask-worker-deployment.yaml',
]

TEMPLATES = jinja2.Environment(loader=jinja2.PackageLoader('compute_tasks', 'templates'))


def queue_from_identifier(identifier):
    module, name = identifier.split('.')

    return QUEUE.get(module.lower(), DEFAULT_QUEUE)


def build_context(identifier, data_inputs, job, user, process):
    data_inputs = celery_app.decoder(data_inputs)

    context = operation.OperationContext.from_data_inputs(identifier, data_inputs)

    extra = {
        'DASK_SCHEDULER': 'dask-scheduler-{!s}.{!s}.svc:8786'.format(user, os.environ['NAMESPACE'])
    }

    logger.info('Append extra %r to frames', extra)

    data = {
        'extra': extra,
        'job': job,
        'user': user,
        'process': process,
    }

    context.init_state(data)

    return context


def validate_workflow(context):
    for next, var_names in context.topo_sort():
        logger.info('Validating %r candidate variable names %r', next.identifier, var_names)

        process = base.get_process(next.identifier)

        process._validate(next)

        if all([isinstance(x, cwt.Variable) for x in next.inputs]):
            if len(var_names) > 1:
                raise base.ValidationError('Expecting the same variable name for all inputs of {!s}, got {!s}', next.identifier, ', '.join(var_names))
        else:
            # TODO validate variable names are available
            pass


def build_workflow(identifier, data_inputs, job, user, process):
    context = build_context(identifier, data_inputs, job, user, process)

    validate_workflow(context)

    started = job_started.s(context).set(**DEFAULT_QUEUE)

    logger.info('Created job started task %r', started)

    queue = queue_from_identifier(identifier)

    logger.info('Using queue %r for process %r', queue, identifier)

    process = base.get_process(identifier)._task.s().set(**queue)

    logger.info('Created process task %r', process)

    succeeded = job_succeeded.s().set(**DEFAULT_QUEUE)

    logger.info('Created job succeeded task %r', succeeded)

    return started | process | succeeded


class State(object):
    def __init__(self):
        logger.info('Procesing current state %s', str(self))

    def on_event(self, **event):
        pass

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.__class__.__name__


class WaitingState(State):
    def build_resources(self, **kwargs):
        templates = [TEMPLATES.get_template(x) for x in TEMPLATE_NAMES]

        data = {
            'image': os.environ['IMAGE'],
            'image_pull_secret': os.environ.get('IMAGE_PULL_SECRET', None),
            'image_pull_policy': os.environ.get('IMAGE_PULL_POLICY', 'Always'),
            'scheduler_cpu': os.environ.get('SCHEDULER_CPU', 1),
            'scheduler_memory': os.environ.get('SCHEDULER_MEMORY', '1Gi'),
            'worker_cpu': os.environ.get('WORKER_CPU', 1),
            'worker_memory': os.environ.get('WORKER_MEMORY', '1Gi'),
            'worker_nthreads': os.environ.get('WORKER_NTHREADS', 4),
            'traffic_type': os.environ.get('TRAFFIC_TYPE', 'development'),
            'dev': os.environ.get('DEV', False),
            'workers': os.environ['WORKERS'],
            'data_claim_name': os.environ.get('DATA_CLAIM_NAME', 'data-pvc'),
        }

        data.update(kwargs)

        return json.dumps([x.render(**data) for x in templates])


    def on_event(self, backend, transition, version, identifier, data_inputs, job, user, process):
        transition = transition.encode()

        if transition == REQUEST:
            try:
                resources = self.build_resources(user=user)

                backend.worker.send_multipart([RESOURCE, resources.encode()])
            except Exception as e:
                logger.exception('Error templating resources')

                backend.fail_job(job, e)

                return self

            return ResourceAckState(identifier, data_inputs, job, user, process)

        logger.info('Invalid transition %r, staying in current state %r', transition, self)

        return self

class ResourceAckState(State):
    def __init__(self, identifier, data_inputs, job, user, process):
        super(ResourceAckState, self).__init__()

        self.identifier = identifier
        self.data_inputs = data_inputs
        self.job = job
        self.user = user
        self.process = process

    def on_event(self, backend, *frames):
        transition = frames[0].encode()

        if transition == ACK:
            try:
                workflow = build_workflow(self.identifier, self.data_inputs, self.job, self.user, self.process)

                workflow.apply_async(serializer='cwt_json')
            except Exception as e:
                logger.exception('Error building and executing workflow')

                backend.fail_job(self.job, e)
        elif transition == ERR:
            backend.fail_job(self.job, frames[1])

        logger.info('Invalid transition %r, staying in current state %r', transition, self)

        return WaitingState()


class Worker(state_mixin.StateMixin, threading.Thread):
    def __init__(self, version, queue_host):
        state_mixin.StateMixin.__init__(self)

        threading.Thread.__init__(self)

        self.version = version

        self.context = None

        self.worker = None

        self.poller = None

        self.heartbeat_at = None

        self.running = True

        self.liveness = HEARTBEAT_LIVENESS

        self.interval = INTERVAL_INIT

        self.queue_host = queue_host or os.environ['PROVISIONER_BACKEND']

    def initialize(self):
        """ Initializes the worker.
        """
        self.context = zmq.Context(1)

        self.poller = zmq.Poller()

        self.init_api()

        base.discover_processes()

        self.state = WaitingState()

    def stop(self):
        self.running = False

        self.join()

        self.worker.close(0)

        self.context.destroy(0)

    def connect_provisioner(self):
        self.worker = self.context.socket(zmq.DEALER)

        SNDTIMEO = os.environ.get('SEND_TIMEOUT', 15)
        RCVTIMEO = os.environ.get('RECV_TIMEOUT', 15)

        self.worker.setsockopt(zmq.SNDTIMEO, SNDTIMEO * 1000)
        self.worker.setsockopt(zmq.RCVTIMEO, RCVTIMEO * 1000)
        self.worker.setsockopt(zmq.LINGER, 0)

        self.worker.connect('tcp://{!s}'.format(self.queue_host))

        logger.info('Created dealer socket and connected to %r', self.queue_host)

        self.poller.register(self.worker, zmq.POLLIN)

        try:
            self.worker.send_multipart([READY, self.version])
        except zmq.Again:
            logger.info('Error notifying provisioner of READY state')

            self.disconnect_provisioner()
        else:
            logger.info('Notified provisioner, in READY state')

    def disconnect_provisioner(self):
        self.poller.unregister(self.worker)

        self.worker.close(0)

    def reconnect_provisioner(self):
        self.disconnect_provisioner()

        self.connect_provisioner()

    def fail_job(self, job, e):
        try:
            self.job = job

            self.failed(str(e))
        except Exception:
            pass
        finally:
            self.job = None

    def send_heartbeat(self):
        if time.time() > self.heartbeat_at:
            self.heartbeat_at = time.time() + HEARTBEAT_INTERVAL

            logger.debug('Sending heartbeat to queue')

            try:
                self.worker.send_multipart([HEARTBEAT, self.version])
            except zmq.Again:
                logger.info('Error sending heartbeat to provisioner')

                raise Exception()

    def missed_heartbeat(self):
        self.liveness -= 1

        if self.liveness == 0:
            logger.error('Heartbeat failed cannot reach queue')
            logger.error('Reconnect in %r', self.interval)

            time.sleep(self.interval)

            if self.interval < INTERVAL_MAX:
                self.interval *= 2

            self.reconnect_provisioner()

            self.liveness = HEARTBEAT_LIVENESS

    def run(self):
        self.initialize()

        self.connect_provisioner()

        self.heartbeat_at = time.time() + HEARTBEAT_INTERVAL

        while self.running:
            socks = dict(self.poller.poll(HEARTBEAT_INTERVAL))

            if socks.get(self.worker) == zmq.POLLIN:
                try:
                    frames = self.worker.recv_multipart()
                except zmq.Again:
                    logger.info('Error receiving data from provisioner')
                else:
                    if frames[0] == HEARTBEAT:
                        self.liveness = HEARTBEAT_LIVENESS

                        logger.info('Received heartbeat setting liveness to %r', self.liveness)
                    else:
                        frames = [x.decode() for x in frames]

                        self.state = self.state.on_event(self, *frames)

                        logger.info('Transitioning state %r', frames)

                    self.interval = INTERVAL_INIT
            else:
                try:
                    self.missed_heartbeat()
                except Exception:
                    self.reconnect_provisioner()

                    continue

            self.send_heartbeat()

        logger.info('Thread is finished')


def register_processes():
    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    parser.add_argument('--dry-run', help='Run without actually doing anything', action='store_true')

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    if not args.dry_run:
        state = state_mixin.StateMixin()

        state.init_api()

    for item in base.discover_processes():
        process = base.get_process(item['identifier'])

        item['abstract'] = process._render_abstract()

        logger.debug('Abstract %r', item['abstract'])

        if not args.dry_run:
            try:
                state.register_process(**item)
            except state_mixin.ProcessExistsError:  # pragma: no cover
                logger.info('Process %r already exists', item['identifier'])

                pass


def parse_args():  # pragma: no cover
    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    parser.add_argument('--queue-host', help='Queue to communicate with')

    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(level=args.log_level)

    worker = Worker(b'devel', args.queue_host)

    worker.start()
