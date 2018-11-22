#! /usr/bin/env python

import glob
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

    data_inputs = '[';

    parts = operation.identifier.split('.')

    operation = cwt.Process(parts[1].replace('-', '.'))

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

def edas_wait(socket):
    if socket.poll(settings.WPS_EDAS_TIMEOUT*1000, zmq.POLLIN) == 0:
        raise WPSError('Timed out waiting for response')

    data = socket.recv()

    parts = data.split('!')

    check_error(parts)

    return parts

def check_error(message):
    if len(message) > 2 and message[1] == 'error':
        raise WPSError('EDASK failed %r', message[2])

def edas_send(socket, message):
    socket.send(message)

    return edas_wait(socket)

def set_output(job, var_name, filename):
    glob_pattern = '{}/results/*/*/*{}'.format(settings.WPS_EDAS_OUTPUT_PATH,
                                         filename)

    try:
        file_path = glob.glob(glob_pattern)[0]
    except IndexError:
        raise WPSError('Could not find output file')

    new_filename = '{}.nc'.format(uuid.uuid4())

    output_path = os.path.join(settings.WPS_PUBLIC_PATH, new_filename)

    try:
        shutil.move(file_path, output_path)
    except OSError:
        raise WPSError('Failed to copy EDASK output')

    output_url = settings.WPS_DAP_URL.format(filename=new_filename)

    output = cwt.Variable(output_url, var_name)

    job.succeeded(json.dumps(output.parameterize()))

@base.cwt_shared_task()
def edas_submit(self, variable, domain, operation, user_id, job_id):
    job = self.load_job(job_id)

    data_inputs = prepare_data_inputs(variable, domain, operation)

    context = zmq.Context.instance()

    with connect_socket(context, zmq.PULL, settings.WPS_EDAS_HOST,
                        settings.WPS_EDAS_RES_PORT) as pull_sock:

        with connect_socket(context, zmq.REQ, settings.WPS_EDAS_HOST,
                            settings.WPS_EDAS_REQ_PORT) as req_sock:
            # Hand empty extras, they're getting ignored
            extras = json.dumps({})

            message = '{}!execute!{}!{}!{}'.format(self.request.id,
                                                   operation.identifier, data_inputs, extras)

            response = edas_send(req_sock, message)

        response = edas_wait(pull_sock)

    _, _, header = response[:3]

    parts = header.split('|')

    set_output(job, parts[-2], parts[-1])
