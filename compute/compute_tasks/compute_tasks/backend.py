import logging
import os

import zmq

import compute_tasks # noqa
from compute_tasks import base
from compute_tasks import celery # noqa
from compute_tasks.job import job_started
from compute_tasks.job import job_succeeded
from compute_tasks.context import StateMixin
from compute_tasks.context import ProcessExistsError

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


def get_next_request(worker):
    logger.info('Notifying provisioner, ready for work')

    # TODO need to get version from somewhere
    worker.send_multipart([b'READY', b'devel'])

    msg = worker.recv_multipart()

    msg = [x.decode() for x in msg[2:]]

    logger.info('Received request from provisioner %r', msg)

    return msg


def fail_job(state, job, e):
    try:
        state.job = job

        state.failed(str(e))
    except Exception:
        pass
    finally:
        state.job = None


def main():
    logging.basicConfig(level=logging.INFO)

    state = StateMixin()

    state.init_api()

    for item in base.discover_processes():
        try:
            state.register_process(**item)
        except ProcessExistsError:
            logger.info('Process %r already exists', item['identifier'])

    base.build_process_bindings()

    context = zmq.Context(1)

    worker = context.socket(zmq.REQ)

    worker.setsockopt(zmq.IDENTITY, HOSTNAME.encode())

    worker.connect('tcp://{!s}'.format(PROVISIONER_BACKEND).encode())

    while True:
        version, identifier, data_inputs, job, user, process = get_next_request(worker)

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

            workflow.delay()

            logger.info('Executed workflow')
        except Exception as e:
            logger.exception('Error executing celery workflow %r', e)

            fail_job(state, job, e)
