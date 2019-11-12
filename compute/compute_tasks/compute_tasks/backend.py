import argparse
import json
import logging
import os
from functools import partial

import jinja2

from compute_tasks import base
from compute_tasks import celery_ as celery
from compute_tasks.job import job_started
from compute_tasks.job import job_succeeded
from compute_tasks.context import state_mixin
from compute_provisioner.worker import Worker
from compute_provisioner.worker import REQUEST_TYPE
from compute_provisioner.worker import RESOURCE_TYPE
from compute_provisioner.worker import ERROR_TYPE

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


def queue_from_identifier(identifier):
    module, name = identifier.split('.')

    return QUEUE.get(module.lower(), DEFAULT_QUEUE)


def fail_job(state, job, e):
    try:
        state.job = job

        state.failed(str(e))
    except Exception:
        pass
    finally:
        state.job = None


def format_frames(frames):
    logger.debug('Handling frames %r', frames)

    version, identifier, data_inputs, job, user, process = [x.decode() for x in frames]

    data_inputs = celery.decoder(data_inputs)

    logger.info('Building celery workflow')

    extra = {
        'DASK_SCHEDULER': 'dask-scheduler-{!s}.{!s}.svc:8786'.format(user, os.environ['NAMESPACE']),
    }

    return identifier, data_inputs, job, user, process, extra


def build_workflow(frames):
    started = job_started.s(*frames).set(**DEFAULT_QUEUE)

    logger.info('Created job started task %r', started)

    queue = queue_from_identifier(frames[0])

    logger.info('Using queue %r', queue)

    process = base.get_process(frames[0]).s().set(**queue)

    logger.info('Found process %r for %r', frames[4], frames[0])

    succeeded = job_succeeded.s().set(**DEFAULT_QUEUE)

    logger.info('Created job stopped task %r', succeeded)

    return started | process | succeeded


def request_handler(frames, state):
    try:
        frames = format_frames(frames)

        workflow = build_workflow(frames)

        logger.info('Built workflow %r', workflow)

        workflow.delay()

        logger.info('Executed workflow')
    except Exception as e:
        logger.exception('Error executing celery workflow %r', e)

        fail_job(state, frames[2], e)


def resource_request(frames, env):
    version, identifier, data_inputs, job, user, process = [x.decode() for x in frames]

    resources = []

    data = {
        'image': os.environ['IMAGE'],
        'image_pull_secret': os.environ.get('IMAGE_PULL_SECRET', None),
        'image_pull_policy': os.environ.get('IMAGE_PULL_POLICY', 'Always'),
        'dev': os.environ.get('DEV', False),
        'user': user,
        'workers': os.environ['WORKERS'],
        'data_claim_name': os.environ.get('DATA_CLAIM_NAME', 'data-pvc'),
    }

    dask_scheduler_pod = env.get_template('dask-scheduler-pod.yaml')

    dask_scheduler_service = env.get_template('dask-scheduler-service.yaml')

    dask_worker_deployment = env.get_template('dask-worker-deployment.yaml')

    resources.append(dask_scheduler_pod.render(**data))

    resources.append(dask_scheduler_service.render(**data))

    resources.append(dask_worker_deployment.render(**data))

    return json.dumps(resources)


def error_handler(frames, state):
    version, identifier, data_inputs, job, user, process = [x.decode() for x in frames[:-2]]

    logger.error('Resource allocation failed %r', frames[-1])

    fail_job(state, job, frames[-1].decode())


def load_processes(state, register_tasks=True):
    if register_tasks:
        for item in base.discover_processes():
            try:
                state.register_process(**item)
            except state_mixin.ProcessExistsError:  # pragma: no cover
                logger.info('Process %r already exists', item['identifier'])

                pass

    base.build_process_bindings()


def reload_argparse():  # pragma: no cover
    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    return parser


def reload_processes():
    parser = reload_argparse()

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    state = state_mixin.StateMixin()

    state.init_api()

    load_processes(state)


def backend_argparse():  # pragma: no cover
    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    parser.add_argument('--queue-host', help='Queue to communicate with')

    parser.add_argument('--skip-register-tasks', help='Skip registering Celery tasks', action='store_true')

    parser.add_argument('--skip-init-api', help='Skip initializing API', action='store_true')

    parser.add_argument('-d', help='Development mode', action='store_true')

    return parser


def main():
    parser = backend_argparse()

    args = parser.parse_args()

    register_tasks = not (args.skip_register_tasks or args.d)

    init_api = not (args.skip_init_api or args.d)

    logging.basicConfig(level=args.log_level)

    logger.debug('CLI Arguments %r', args)

    logger.info('Loading templates')

    env = jinja2.Environment(loader=jinja2.PackageLoader('compute_tasks', 'templates'))

    state = state_mixin.StateMixin()

    if init_api:
        state.init_api()

    load_processes(state, register_tasks)

    # Need to get the supported version from somewhere
    # environment variable or hard code?
    worker = Worker(b'devel')

    queue_host = args.queue_host or os.environ['PROVISIONER_BACKEND']

    request_handler_partial = partial(request_handler, state=state)

    request_resource_partial = partial(resource_request, env=env)

    error_handler_partial = partial(error_handler, state=state)

    def callback_handler(type, frames):  # pragma: no cover
        if type == REQUEST_TYPE:
            value = request_handler_partial(frames)
        elif type == RESOURCE_TYPE:
            value = request_resource_partial(frames)
        elif type == ERROR_TYPE:
            value = error_handler_partial(frames)
        else:
            logger.error('Could not handle unknown type %r', type)

            raise Exception('Could not handle unknown type {!r}'.format(type))

        return value

    worker.run(queue_host, callback_handler)
