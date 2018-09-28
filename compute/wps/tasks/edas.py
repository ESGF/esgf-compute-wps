#! /usr/bin/env python

import json
import os
import shutil
import uuid
from xml.etree import ElementTree as ET

import cdms2
import cwt
import zmq
from celery.utils.log import get_task_logger
from django.conf import settings

from wps import WPSError
from wps.tasks import base

logger = get_task_logger('wps.tasks.edas')

def connect_socket(context, socket_type, host, port):
    try:
        sock = context.socket(socket_type)

        sock.connect('tcp://{}:{}'.format(host, port))
    except zmq.ZMQError:
        raise WPSError('Failed to connect to EDAS {} on port {}', host, port)

    logger.info('Connected to EDASK %r on port %r with type %r', host, port, socket_type)

    return sock

def prepare_data_inputs(variable, domain, operation):
    # TODO Remove when https://github.com/ESGF/esgf-compute-api/issues/39 is
    # resolved.
    class Dummy(object):
        metadata = None

    operation.description = Dummy()

    variable = json.dumps([x.parameterize() for x in variable])

    domain = json.dumps([domain.parameterize()])

    operation = json.dumps([operation.parameterize()])

    data_inputs = '[variable={};domain={};operation={}]'.format(
        variable, domain, operation)

    logger.info('Data Inputs "%r"', data_inputs)

    return data_inputs

def check_error(data):
    logger.info('response: %r', data)

    id, status, message = data.split('!')

    logger.info('id: %r status: %r message: %r', id, status, message)

    if status == 'error':
        raise WPSError('EDASK error: "{}"', message)

@base.cwt_shared_task()
def edas_submit(self, variable, domain, operation, user_id, job_id):
    job = self.load_job(job_id)

    data_inputs = prepare_data_inputs(variable, domain, operation)

    context = zmq.Context.instance()

    with connect_socket(context, zmq.PULL, settings.WPS_EDAS_HOST,
                        settings.WPS_EDAS_RES_PORT) as pull_sock:

        with connect_socket(context, zmq.REQ, settings.WPS_EDAS_HOST,
                            settings.WPS_EDAS_REQ_PORT) as req_sock:
            self.update(job, 'Connected to EDASK backend') 

            extras = json.dumps({})

            req_sock.send('{}!execute!{}!{}!{}'.format(self.request.id, operation.identifier,
                                                       data_inputs, extras))

            if (req_sock.poll(settings.WPS_EDAS_TIMEOUT*1000, zmq.POLLIN) == 0):
                raise WPSError('')

            self.update(job, 'Sent EDASK request')

            data = req_sock.recv()

        if (pull_sock.poll(settings.WPS_EDAS_TIMEOUT*1000, zmq.POLLIN) == 0):
            raise WPSError('')

        data = pull_sock.recv()

        check_error(data)

    return None
