#! /usr/bin/env python

import glob
import json
import os
import shutil
import time
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

    return sock

def prepare_data_inputs(variable, domain, operation):
    # TODO Remove when https://github.com/ESGF/esgf-compute-api/issues/39 is
    # resolved.
    class Dummy(object):
        metadata = None

    operation.description = Dummy()

    data_inputs = '[';

    operation.inputs = [x.name for x in variable]

    variable = [x.parameterize() for x in variable]

    if len(variable) > 0:
        data_inputs = '{}variable = {}'.format(data_inputs,
                                               json.dumps(variable))

    if domain is None:
        domain = cwt.Domain()

    operation.domain = domain.name

    data_inputs = '{};domain = {}'.format(data_inputs,
                                          json.dumps([domain.parameterize()]))

    data_inputs = '{};operation = {}]'.format(data_inputs,
                                              json.dumps([operation.parameterize()]))

    logger.info('Data Inputs "%r"', data_inputs)

    return data_inputs

def edas_peek(data):
    n = min(len(data), 400)

    return data[:n]

def edas_wait(socket):
    if socket.poll(settings.WPS_EDAS_TIMEOUT*1000, zmq.POLLIN) == 0:
        raise WPSError('Timed out waiting for response')

    data = socket.recv()

    logger.info('Received data, length %r, peek %r', len(data), edas_peek(data))

    return data

def edas_send(req_socket, pull_socket, message):
    req_socket.send(message)

    logger.info('Send message: %r', message)

    return edas_wait(pull_socket)

def edas_result(pull_socket):
    data = edas_wait(pull_socket)

    try:
        id, type, msg = data.split('!')
    except ValueError:
        raise WPSError('Failed to parse EDASK response, expected 3 tokens')

    parts = msg.split('|')

    try:
        output = set_output(parts[-3], parts[-1])
    except IndexError:
        raise WPSError('Failed to set the output of the EDASK operation')

    return output

def set_output(var_name, file_path):
    new_filename = '{}.nc'.format(uuid.uuid4())

    output_path = os.path.join(settings.WPS_PUBLIC_PATH, new_filename)

    try:
        shutil.move(file_path, output_path)
    except OSError:
        raise WPSError('Failed to copy EDASK output')

    output_url = settings.WPS_DAP_URL.format(filename=new_filename)

    output = cwt.Variable(output_url, var_name)

    return output

@base.cwt_shared_task()
def edas_submit(self, variable, domain, operation, user_id, job_id):
    job = self.load_job(job_id)

    parts = operation.identifier.split('.')

    operation = cwt.Process(parts[1].replace('-', '.'))

    data_inputs = prepare_data_inputs(variable, domain, operation)

    context = zmq.Context.instance()

    with connect_socket(context, zmq.PULL, settings.WPS_EDAS_HOST,
                        settings.WPS_EDAS_RES_PORT) as pull_sock:

        self.update(job, 'Connected to EDASK pull socket')

        with connect_socket(context, zmq.REQ, settings.WPS_EDAS_HOST,
                            settings.WPS_EDAS_REQ_PORT) as req_sock:

            self.update(job, 'Connected to EDASK request socket')

            extras = json.dumps({
                'storeExecuteResponse': 'false',
                'status': 'true',
                'responseform': 'file',
                'sendData': 'false',
            })

            message = '{}!execute!{}!{}!{}'.format(job_id,
                                                   operation.identifier, data_inputs, extras)

            edas_send(req_sock, req_sock, message)

            self.update(job, 'Sent {!r} byte request to EDASK', len(message))

        output = edas_result(pull_sock)

        self.update(job, 'Completed with output {!r}', output)

        job.succeeded(json.dumps(output.parameterize()))
