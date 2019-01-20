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

    try:
        operation.domain = domain.name
    except AttributeError:
        raise WPSError('EDASK requires that a domain be supplied')

    data_inputs = '{};domain = {}'.format(data_inputs,
                                          json.dumps([domain.parameterize()]))

    data_inputs = '{};operation = {}]'.format(data_inputs,
                                              json.dumps([operation.parameterize()]))

    logger.info('Data Inputs "%r"', data_inputs)

    return data_inputs

def edas_peek(data):
    n = min(len(data), 400)

    return data[:n]

def edas_wait(socket, timeout):
    if socket.poll(timeout*1000, zmq.POLLIN) == 0:
        raise WPSError('Timed out waiting for response')

    data = socket.recv()

    logger.info('Received data, length %r, peek %r', len(data), edas_peek(data))

    return data

def edas_send(req_socket, pull_socket, message):
    req_socket.send(message)

    logger.info('Send message: %r', message)

    return edas_wait(pull_socket, settings.WPS_EDAS_QUEUE_TIMEOUT)

def edas_result(pull_socket):
    data = edas_wait(pull_socket, settings.WPS_EDAS_EXECUTE_TIMEOUT)

    try:
        id, type, msg = data.split('!')
    except ValueError:
        raise WPSError('Failed to parse EDASK response, expected 3 tokens')

    if type == 'error':
        raise WPSError(msg)

    parts = msg.split('|')

    var_name = parts[-3].replace('[', '%5b')

    var_name = var_name.replace(']', '%5d')

    try:
        output = set_output(var_name, parts[-1])
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
def edas_submit(self, context):
    params = context.operation.parameters

    parts = context.operation.identifier.split('.')

    operation = cwt.Process(parts[1].replace('-', '.'))

    operation.parameters = params

    variables = [x.variable for x in context.inputs]

    data_inputs = prepare_data_inputs(variables, context.domain, operation)

    zmq_context = zmq.Context.instance()

    with connect_socket(zmq_context, zmq.PULL, settings.WPS_EDAS_HOST,
                        settings.WPS_EDAS_RES_PORT) as pull_sock:

        self.status('Connected to EDASK pull socket')

        with connect_socket(zmq_context, zmq.REQ, settings.WPS_EDAS_HOST,
                            settings.WPS_EDAS_REQ_PORT) as req_sock:

            self.status('Connected to EDASK request socket')

            extras = json.dumps({
                'storeExecuteResponse': 'true',
                'status': 'true',
                'responseform': 'xml',
                'sendData': 'false',
            })

            message = '{}!execute!{}!{}!{}'.format(context.job.id, operation.identifier, data_inputs, extras)

            edas_send(req_sock, req_sock, message)

            self.status('Sent {!r} byte request to EDASK', len(message))

        output = edas_result(pull_sock)

        self.status('Completed with output {!r}', output)

        context.output_data = json.dumps(output.parameterize())

    return context
