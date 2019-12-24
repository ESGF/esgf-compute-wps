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
from compute_tasks import celery_ as celery
from compute_tasks import WPSError
from compute_tasks.job import job_started
from compute_tasks.job import job_succeeded
from compute_tasks.context import state_mixin

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


def validate_process(process):
    logger.info('Validating process %r', process)

    v = cdat.VALIDATION[process.identifier]

    if len(process.inputs) < v['min'] or len(process.inputs) > v['max']:
        raise WPSError('Validation failed, expected the number of inputs to be between {!s} and {!s}', v['min'], v['max'])

    for key, param_v in v['params'].items():
        required = param_v['required']

        param = process.parameters.get(key, None)

        if param is None and required:
            raise WPSError('Validation failed, expected parameter {!r}', key)

        if param is not None:
            type = param_v['type']

            for value in param.values:
                try:
                    type(value)
                except ValueError:
                    raise WPSError('Validation failed, could not convert parameter {!r} value {!r} to type {!r}', key, value, type.__name__)


def build_workflow(*frames):
    frames = list(frames)

    frames[1] = celery.decoder(frames[1])

    for data in frames[1]['operation']:
        process = cwt.Process.from_dict(data)

        validate_process(process)

    extra = {
        'DASK_SCHEDULER': 'dask-scheduler-{!s}.{!s}.svc:8786'.format(frames[4], os.environ['NAMESPACE'])
    }

    frames.append(extra)

    logger.info('Append extra %r to frames', extra)

    started = job_started.s(*frames).set(**DEFAULT_QUEUE)

    logger.info('Created job started task %r', started)

    queue = queue_from_identifier(frames[0])

    logger.info('Using queue %r for process %r', queue, frames[0])

    process = base.get_process(frames[0]).s().set(**queue)

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
    def on_event(self, backend, new_state, version, identifier, data_inputs, job, user, process):
        if new_state == REQUEST:
            try:
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
                    'user': user.decode(),
                    'workers': os.environ['WORKERS'],
                    'data_claim_name': os.environ.get('DATA_CLAIM_NAME', 'data-pvc'),
                }

                resources = json.dumps([x.render(**data) for x in templates])

                backend.worker.send_multipart([RESOURCE, resources.encode()])
            except Exception as e:
                logger.exception('Error templating resources')

                backend.fail_job(job, e)

                return self

            return ResourceAckState(identifier, data_inputs, job, user, process)

        logger.info('Invalid transition, staying in current state')

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
        if frames[0] == ACK:
            try:
                workflow = build_workflow(self.identifier, self.data_inputs, self.job, self.user, self.process)

                workflow.delay()

                backend.worker.send_multipart([ACK])
            except Exception as e:
                backend.fail_job(self.job, e)

            return WaitingState()
        elif frames[0] == ERR:
            backend.fail_job(self.job, frames[1])

            return WaitingState()

        logger.info('Invalid transition, staying in current state')

        return self

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

        self.exc = None

    def initialize(self):
        """ Initializes the worker.
        """
        self.context = zmq.Context(1)

        self.poller = zmq.Poller()

        self.init_api()

        base.discover_processes()

        base.build_process_bindings()

        self.state = WaitingState()

    def stop(self):
        self.running = False

        self.join()

        self.worker.close(0)

        self.context.destroy(0)

    def connect_provisioner(self):
        self.worker = self.context.socket(zmq.DEALER)

        self.worker.connect('tcp://{!s}'.format(self.queue_host))

        logger.info('Created dealer socket and connected to %r', self.queue_host)

        self.poller.register(self.worker, zmq.POLLIN)

        self.worker.send_multipart([READY, self.version])

        logger.info('Notifying queue, ready for work')

    def reconnect_provisioner(self):
        self.poller.unregister(self.worker)

        self.worker.setsockopt(zmq.LINGER, 0)

        self.worker.close(0)

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

            self.worker.send_multipart([HEARTBEAT, self.version])

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
        try:
            self.initialize()

            self.connect_provisioner()

            self.heartbeat_at = time.time() + HEARTBEAT_INTERVAL

            while self.running:
                socks = dict(self.poller.poll(HEARTBEAT_INTERVAL))

                if socks.get(self.worker) == zmq.POLLIN:
                    frames = self.worker.recv_multipart()

                    logger.info('Handling frames from provisioner %r', frames)

                    # Heartbeats dont alter state. Maybe they should?
                    if frames[0] == HEARTBEAT:
                        self.liveness = HEARTBEAT_LIVENESS

                        logger.debug('Received heartbeat from queue, setting liveness to %r', self.liveness)
                    else:
                        frames = [x.decode() for x in frames]

                        self.state = self.state.on_event(self, *frames)

                    self.interval = INTERVAL_INIT
                else:
                    self.missed_heartbeat()

                self.send_heartbeat()

            logger.info('Thread is finished')
        except Exception as e:
            self.exc = e


def register_processes():
    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    state = state_mixin.StateMixin()

    state.init_api()

    for item in base.discover_processes():
        try:
            state.register_process(**item)
        except state_mixin.ProcessExistsError:  # pragma: no cover
            logger.info('Process %r already exists', item['identifier'])

            pass

    base.build_process_bindings()


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
