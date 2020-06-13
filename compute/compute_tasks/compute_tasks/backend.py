import argparse
import hashlib
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
from dask import utils
from prometheus_client import start_http_server

from compute_tasks import base
from compute_tasks import cdat
from compute_tasks import celery_app
from compute_tasks import mapper
from compute_tasks import metrics
from compute_tasks import WPSError
from compute_tasks.context import OperationContext
from compute_tasks.context.tracker_api import ProcessExistsError
from compute_tasks.context.tracker_api import TrackerAPI
from compute_tasks.job import job_started
from compute_tasks.job import job_succeeded

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
HEARTBEAT_INTERVAL = 4.0

INTERVAL_INIT = 1
INTERVAL_MAX = 32

TEMPLATE_NAMES = [
    'dask-kubernetes-configmap.yaml',
    'dask-kubernetes-service.yaml',
    'dask-kubernetes-pod.yaml',
]

def sha256sum(x):
    if not isinstance(x, bytes):
        x = x.encode()

    return hashlib.sha256(x).hexdigest()

TEMPLATES = jinja2.Environment(loader=jinja2.PackageLoader('compute_tasks', 'templates'), undefined=jinja2.DebugUndefined)
TEMPLATES.filters['sha256sum'] = sha256sum

def int_or_float(x):
    try:
        return int(x)
    except ValueError:
        return float(x)

def determine_user_resources():
    limit_cpu = int_or_float(os.environ.get('USER_LIMIT_CPU', 2))
    limit_memory = utils.parse_bytes(os.environ.get('USER_LIMIT_MEMORY', '2Gi'))

    logger.info(f'User resource limits cpu: {limit_cpu!r} memory: {limit_memory!r}')

    scheduler_cpu = int_or_float(os.environ.get('SCHEDULER_CPU', 1))
    scheduler_memory = utils.parse_bytes(os.environ.get('SCHEDULER_MEMORY', '1Gi'))

    logger.info(f'Scheduler resources cpu: {scheduler_cpu!r} memory: {scheduler_memory!r}')

    assert limit_cpu > scheduler_cpu, f'USER_LIMIT_CPU must be greater than SCHEDULER_CPU'
    assert limit_memory > scheduler_memory, f'USER_LIMIT_MEMORY must be greater than SCHEDULER_MEMORY'

    workers = int(os.environ.get('WORKERS', 2))
    worker_nthreads = int(os.environ.get('WORKER_NTHREADS', 1))

    worker_cpu = (limit_cpu - scheduler_cpu) / workers
    worker_memory = (limit_memory - scheduler_memory) / workers

    logger.info(f'Using {workers!r} worker with resources threads: {worker_nthreads} cpu: {worker_cpu!r} memory: {worker_memory!r}')

    resources = {
        'scheduler_cpu': scheduler_cpu,
        'scheduler_memory': scheduler_memory,
        'workers': workers,
        'worker_cpu': worker_cpu,
        'worker_memory': worker_memory,
        'worker_nthreads': worker_nthreads,
    }

    return resources

def queue_from_identifier(identifier):
    module, name = identifier.split('.')

    return QUEUE.get(module.lower(), DEFAULT_QUEUE)


def build_context(identifier, data_inputs, job, user, process, status, namespace, **extra):
    ctx = OperationContext.from_data_inputs(identifier, data_inputs)

    extra.update(DASK_SCHEDULER = f'dask-scheduler-{user!s}.{namespace!s}.svc:8786')

    logger.info(f'Built operation context with extra {extra!r}')

    data = {
        'extra': extra,
        'job': job,
        'user': user,
        'process': process,
        'status': status,
    }

    ctx.init_state(data)

    logger.info(f'Initialized state with {data!r}')

    return ctx


def build_workflow(identifier, data_inputs, job, user, process, status, **kwargs):
    ctx = build_context(identifier, data_inputs, job, user, process, status, **kwargs)

    base.validate_workflow(ctx)

    started = job_started.s(ctx).set(**DEFAULT_QUEUE)

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
        pass

    def on_event(self, **event):
        pass

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.__class__.__name__


def render_templates(**kwargs):
    map = mapper.Mapper.from_config('/etc/config/mapping.json')

    templates = dict((x, TEMPLATES.get_template(x)) for x in TEMPLATE_NAMES)

    data = {
        'image': os.environ['IMAGE'],
        'redis_host': os.environ['REDIS_HOST'],
        'redis_port': os.environ['REDIS_PORT'],
        'redis_db': os.environ['REDIS_DB'],
        'image_pull_policy': os.environ.get('IMAGE_PULL_POLICY', 'Always'),
        'worker_redis_cache_enabled': bool(os.environ.get('WORKER_REDIS_CACHE_ENABLED', False)),
        'traffic_type': os.environ.get('TRAFFIC_TYPE', 'development'),
        'dev': os.environ.get('DEV', False),
        'data_claim_name': os.environ.get('DATA_CLAIM_NAME', 'data-pvc'),
        'volumes': map.mounts,
    }

    data.update(kwargs)

    user_resources = determine_user_resources()

    data.update(user_resources)

    return dict((x, y.render(**data)) for x, y in templates.items())


class WaitingState(State):
    def on_event(self, backend, transition, version, payload):
        # TODO fix constants to compare str
        transition = transition.encode()

        payload = json.loads(payload)

        logger.info(f'Current state {self!s} transition to {transition!s}')

        if transition == REQUEST:
            try:
                resources = json.dumps(list(render_templates(user=payload['user']).values()))

                backend.worker.send_multipart([RESOURCE, resources.encode()])
            except Exception as e:
                backend.fail_job(payload['job'], e)

                logger.exception(f'Failed job, error building resources')
            else:
                logger.info(f'Setting new state to ResourceAckState')

                return ResourceAckState(payload)
        else:
            logger.info(f'Transition is invalid resetting to WaitingState')

        return self

class ResourceAckState(State):
    def __init__(self, payload):
        super(ResourceAckState, self).__init__()

        self.payload = payload

    def on_event(self, backend, transition, data):
        transition = transition.encode()

        logger.info(f'Current state {self!s} transitioning to {transition!s}')

        if transition == ACK:
            metrics.BACKEND_RESOURCE_RESPONSE.labels('success').inc()

            self.payload.update(json.loads(data))

            try:
                workflow = build_workflow(**self.payload)

                workflow.apply_async(serializer='cwt_json')
            except Exception as e:
                backend.fail_job(self.payload['job'], e)

                logger.exception(f'Failed job, error building workflow')
        elif transition == ERR:
            metrics.BACKEND_RESOURCE_RESPONSE.labels('error').inc()

            backend.fail_job(self.payload['job'], data)

            logger.info(f'Failed job, error allocating resources')
        else:
            metrics.BACKEND_RESOURCE_RESPONSE.labels('unknown').inc()

            logger.info(f'Transition is invalid resetting to WaitingState')

        return WaitingState()


class Worker(TrackerAPI, threading.Thread):
    def __init__(self, version, queue_host):
        TrackerAPI.__init__(self)

        threading.Thread.__init__(self)

        self.version = version

        self.ctx = None

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
        self.ctx = zmq.Context(1)

        self.poller = zmq.Poller()

        self.init_api()

        base.discover_processes()

        self.state = WaitingState()

    def stop(self):
        self.running = False

        self.join()

        self.worker.close(0)

        self.ctx.destroy(0)

    def connect_provisioner(self):
        self.worker = self.ctx.socket(zmq.DEALER)

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
        metrics.BACKEND_PROVISIONER_RECONNECT.inc()

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

                # Might want to replace with sys.exit, in this state we may need to kill the container.
                raise Exception()

    def missed_heartbeat(self):
        self.liveness -= 1

        if self.liveness == 0:
            metrics.BACKEND_MISSED_HEARTBEAT.inc()

            logger.info(f'Missed provisioner heartbeat {HEARTBEAT_LIVENESS!r} times sleeping for {self.interval!r} seconds before reconnecting')

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
            socks = dict(self.poller.poll(HEARTBEAT_INTERVAL*1000))

            if socks.get(self.worker) == zmq.POLLIN:
                try:
                    frames = self.worker.recv_multipart()
                except zmq.Again:
                    logger.info('Error receiving data from provisioner')
                else:
                    if frames[0] == HEARTBEAT:
                        metrics.BACKEND_MISSED_HEARTBEAT.set(0)

                        self.liveness = HEARTBEAT_LIVENESS

                        logger.debug('Received heartbeat setting liveness to %r', self.liveness)
                    else:
                        frames = [x.decode() for x in frames]

                        with metrics.BACKEND_STATE_PROCESS_DURATION.labels(str(self.state)).time():
                            self.state = self.state.on_event(self, *frames)

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
        tracker = TrackerAPI()

        tracker.init_api()

    for item in base.discover_processes():
        process = base.get_process(item['identifier'])

        item['abstract'] = process._render_abstract()

        logger.debug('Abstract %r', item['abstract'])

        if not args.dry_run:
            try:
                tracker.register_process(**item)
            except ProcessExistsError:  # pragma: no cover
                logger.info('Process %r already exists', item['identifier'])

                pass


def template():
    parser = argparse.ArgumentParser()

    parser.add_argument('--output-dir', required=True)

    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    data = render_templates(user=0)

    for x, y in data.items():
        with open(os.path.join(args.output_dir, x), 'w') as f:
            f.write(y)


def parse_args():  # pragma: no cover
    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    parser.add_argument('--queue-host', help='Queue to communicate with')

    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(level=args.log_level)

    start_http_server(8888, '0.0.0.0', registry=metrics.backend_registry)

    logger.info('Started metrics server')

    worker = Worker(b'devel', args.queue_host)

    worker.start()

    logger.info('Started main thread')

    worker.join()

    logger.info('Thread exited')
