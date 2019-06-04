import logging
import os
from functools import partial

import compute_tasks # noqa
from compute_tasks import base
from compute_tasks import celery # noqa
from compute_tasks.job import job_started
from compute_tasks.job import job_succeeded
from compute_tasks.context import StateMixin
from compute_tasks.context import ProcessExistsError
from compute_provisioner.worker import Worker

logger = logging.getLogger('compute_tasks.backend')

PROVISIONER_BACKEND = os.environ['PROVISIONER_BACKEND']

HOSTNAME = os.environ['HOSTNAME']

# Set INGRESS_QUEUE to prevent breaking old code
DEFAULT_QUEUE = {
    'queue': 'ingress',
    'exchange': 'ingress',
    'routing_key': 'ingress',
}

QUEUE = {
    'edas': {
        'queue': 'edask',
        'exchange': 'edask',
        'routing_key': 'edask',
    },
    'default': {
        'queue': 'default',
        'exchange': 'default',
        'routing_key': 'default',
    },
}


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


def request_handler(frames, state):
    version, identifier, data_inputs, job, user, process = [x.decode() for x in frames[2:]]

    data_inputs = compute_tasks.decoder(data_inputs)

    logger.info('Building celery workflow')

    try:
        started = job_started.s(identifier, data_inputs, job, user, process).set(**DEFAULT_QUEUE)

        logger.info('Created job started task %r', started)

        queue = queue_from_identifier(identifier)

        logger.info('Using queue %r', queue)

        process = base.get_process(identifier).s().set(**queue)

        logger.info('Found process %r for %r', process, identifier)

        succeeded = job_succeeded.s().set(**DEFAULT_QUEUE)

        logger.info('Created job stopped task %r', succeeded)

        workflow = started | process | succeeded

        logger.info('Built workflow %r', workflow)

        workflow.delay()

        logger.info('Executed workflow')
    except Exception as e:
        logger.exception('Error executing celery workflow %r', e)

        fail_job(state, job, e)


def main():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    parser.add_argument('--queue-host', help='Queue to communicate with')

    parser.add_argument('--skip-register-tasks', help='Skip registering Celery tasks', action='store_true')

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    logger.info('Args %r', args)

    state = StateMixin()

    state.init_api()

    if not args.skip_register_tasks:
        for item in base.discover_processes():
            try:
                state.register_process(**item)
            except ProcessExistsError:
                logger.info('Process %r already exists', item['identifier'])

                pass

    base.build_process_bindings()

    # Need to get the supported version from somewhere
    # environment variable or hard code?
    worker = Worker(b'devel')

    queue_host = args.queue_host or PROVISIONER_BACKEND

    worker.run(queue_host, partial(request_handler, state=state))
