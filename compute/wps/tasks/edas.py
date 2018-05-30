#! /usr/bin/env python

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
from wps.tasks import process

logger = get_task_logger('wps.tasks.edas')

def check_exceptions(data):
    if '<exceptions>' in data:
        index = data.index('!')

        data = data[index+1:]

        root = ET.fromstring(data)

        exceptions = root.findall('./exceptions/*')

        if len(exceptions) > 0:
            raise WPSError('EDAS exception: {error}', error=exceptions[0].text)

def initialize_socket(context, socket_type, host, port):
    sock = context.socket(socket_type)

    sock.connect('tcp://{}:{}'.format(host, port))

    return sock

def listen_edas_output(self, poller, proc):
    edas_output_path = None

    proc.log('Listening for EDAS status')

    while True:
        events = dict(poller.poll(settings.WPS_EDAS_TIMEOUT * 1000))

        if len(events) == 0:
            raise WPSError('EDAS timed out waiting for heartbear or output message')

        data = events.keys()[0].recv()

        check_exceptions(data)

        parts = data.split('!')

        if 'file' in parts:
            sub_parts = parts[-1].split('|')

            edas_output_path = sub_parts[-1]

            break
        elif 'response' in parts:
            proc.log('EDAS Heartbeat')
        
    proc.log('Received success from EDAS backend')

    return edas_output_path

@base.cwt_shared_task()
def edas_submit(self, parent_variables, variables, domains, operation, user_id, job_id):
    self.PUBLISH = base.ALL

    req_sock = None

    sub_sock = None

    edas_output_path = None

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id)

    proc.job.started()

    v, d, o = self.load(parent_variables, variables, domains, operation)

    domain = d.get(o.domain, None)

    data_inputs = cwt.WPSClient('').prepare_data_inputs(o, {}, domain)

    logger.info('Generated datainputs: {}'.format(data_inputs))

    context = zmq.Context.instance()

    poller = None

    try:
        req_sock = initialize_socket(context, zmq.REQ, settings.WPS_EDAS_HOST, settings.WPS_EDAS_REQ_PORT)

        sub_sock = initialize_socket(context, zmq.SUB, settings.WPS_EDAS_HOST, settings.WPS_EDAS_RES_PORT)

        sub_sock.setsockopt(zmq.SUBSCRIBE, b'{}'.format(proc.job.id))

        poller = zmq.Poller()

        poller.register(sub_sock)

        proc.log('Connected to EDAS backend')

        extra = '{"response":"file"}'

        req_sock.send(str('{}!execute!{}!{}!{}'.format(proc.job.id, o.identifier, data_inputs, extra)))

        if (req_sock.poll(settings.WPS_EDAS_TIMEOUT * 1000) == 0):
            raise WPSError('EDAS timed out waiting for accept response')

        data = req_sock.recv()

        proc.log('Sent request to EDAS backend')

        check_exceptions(data)

        edas_output_path = listen_edas_output(self, poller, proc)

        if edas_output_path is None:
            raise WPSError('Failed to receive output from EDAS')
    except:
        raise
    finally:
        if req_sock is not None:
            req_sock.close()

        if sub_sock is not None:
            if poller is not None:
                poller.unregister(sub_sock)

            sub_sock.close()

    proc.log('Received result from EDAS backend')

    output_name = '{}.nc'.format(str(uuid.uuid4()))

    output_path = os.path.join(settings.WPS_LOCAL_OUTPUT_PATH, output_name)

    shutil.move(edas_output_path, output_path)

    proc.log('Localizing output to THREDDS server')

    if settings.WPS_DAP:
        output_url = settings.WPS_DAP_URL.format(filename=output_name)
    else:
        output_url = settings.WPS_OUTPUT_URL.format(filename=output_name)

    var_name = None

    try:
        with cdms2.open(output_path) as infile:
            var_name = infile.variables.keys()[0]
    except:
        raise WPSError('Failed to determine variable name of the EDAS output')

    proc.log('Variable name from EDAS result "{}"', var_name)

    output_var = cwt.Variable(output_url, var_name, name=o.name)

    return {o.name: output_var.parameterize()}
