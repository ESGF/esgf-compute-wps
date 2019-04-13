#! /usr/bin/env python

import json
import os
import shutil
import xml.etree.ElementTree as ET
from uuid import uuid4

import cwt
import zmq
from celery.utils.log import get_task_logger
from django.conf import settings

from wps import models
from wps import WPSError
from wps.tasks import base

logger = get_task_logger('wps.tasks.edas')

MODULE_WHITELIST = ('xarray', )


def connect_socket(context, socket_type, host, port):
    try:
        sock = context.socket(socket_type)

        sock.connect('tcp://{}:{}'.format(host, port))
    except zmq.ZMQError:
        raise WPSError('Failed to connect to EDAS {} on port {}', host, port)

    return sock


def prepare_data_inputs(variable, domain, operation):
    operation.inputs = [x.name for x in variable]

    variable = json.dumps([x.to_dict() for x in variable])

    if domain is None:
        raise WPSError('EDASK requires a domain to be defined')

    operation.domain = domain.name

    domain = json.dumps([domain.to_dict(), ])

    operation = json.dumps([operation.to_dict(), ])

    data_inputs = '[variable = {!s};domain = {!s}; operation={!s}]'.format(variable, domain, operation)

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

    return data.decode()


def edas_send(req_socket, pull_socket, message):
    req_socket.send_string(message)

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
    new_filename = '{}.nc'.format(uuid4())

    output_path = os.path.join(settings.WPS_PUBLIC_PATH, new_filename)

    try:
        shutil.move(file_path, output_path)
    except OSError:
        raise WPSError('Failed to copy EDASK output')

    output_url = settings.WPS_DAP_URL.format(filename=new_filename)

    output = cwt.Variable(output_url, var_name)

    return output


def get_capabilities(req_sock):
    uid = uuid4()

    req_sock.send_string('{!s}!getcapabilities'.format(uid))

    data = req_sock.recv().decode()

    _, body = data.split('!')

    capabilities = {}

    root = ET.fromstring(body)

    for module in root:
        module_name = module.attrib['name']

        if module_name not in MODULE_WHITELIST:
            logger.debug('Skipping module %r', module_name)

            continue

        logger.debug('Module %r', module_name)

        for kernel in module:
            name = kernel.attrib['name']

            logger.debug('Process %r', name)

            # process_id = '{}.{}'.format(module_name, name)

            # describe = describe_process(req_sock, process_id)

            title = kernel.attrib['title']

            capabilities[name] = {
                'module': module_name,
                'title': title,
            }

    return capabilities


def discover_processes():
    zmq_context = zmq.Context.instance()

    with connect_socket(zmq_context, zmq.REQ, settings.WPS_EDAS_HOST, settings.WPS_EDAS_REQ_PORT) as req_sock:
        capabilities = get_capabilities(req_sock)

        items = []

        for name, value in capabilities.items():
            items.append({
                'identifier': 'EDASK.{!s}-{!s}'.format(value['module'], name),
                'backend': 'EDAS',
                'abstract': '',
                'metadata': json.dumps({}),
            })

    return items


def process_bindings():
    """ Creates process bindings.

    Since the processes supported by EDASK are not known ahead of time
    we create a binding between the registered processes identifier
    and a dynamically generated method which is attached to this module.

    We have a single method that handles communication between the WPS
    service and EDASK backend. To support multiple processes we dynamically
    create new methods using the "wrapper" method which each time its called,
    creates a new method signature unique to the identifier. The wrapped
    method is then wrapped by "cwt_shared_task" allowing it to be registered
    as a celery task.
    """
    def wrapper(func, func_name):
        # Create a new method each time it wraps
        def inner_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Set a unique name for the method
        inner_wrapper.__name__ = func_name

        return inner_wrapper

    from wps.tasks import edas

    bindings = {}

    for item in models.Process.objects.filter(backend='EDAS'):
        name = item.identifier.split('.')[1]

        func_name = name.replace('-', '_')

        # Wrap the new method so it may be registered by Celery
        new_method = base.cwt_shared_task()(wrapper(edas_func, func_name))

        # Set the attribute to the current module
        setattr(edas, func_name, new_method)

        # Add the method to the global bindings dict
        bindings[item.identifier] = getattr(edas, func_name)

    return bindings


def edas_func(self, context):
    identifier = context.operation.identifier

    # Convert identifier back to what EDASK understands
    context.operation._identifier = identifier.split('.')[1].replace('-', '.')

    data_inputs = prepare_data_inputs(context.inputs, context.domain, context.operation)

    zmq_context = zmq.Context.instance()

    with connect_socket(zmq_context, zmq.PULL, settings.WPS_EDAS_HOST,
                        settings.WPS_EDAS_RES_PORT) as pull_sock:

        self.status('Connected to EDASK pull socket')

        with connect_socket(zmq_context, zmq.REQ, settings.WPS_EDAS_HOST,
                            settings.WPS_EDAS_REQ_PORT) as req_sock:

            self.status('Connected to EDASK request socket')

            extras = json.dumps({
                'sendData': 'false',
                'responseform': 'wps',
                'response': 'xml',
                'status': 'true',
            })

            message = '{!s}!execute!{!s}!{!s}!{!s}'.format(context.job.id, identifier, data_inputs, extras)

            edas_send(req_sock, req_sock, message)

            self.status('Sent {!r} byte request to EDASK', len(message))

        output = edas_result(pull_sock)

        self.status('Completed with output {!r}', output)

        context.output_data = json.dumps(output.parameterize())

    return context
