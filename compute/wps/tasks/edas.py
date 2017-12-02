#! /usr/bin/env python

import shutil
from xml.etree import ElementTree as ET

import cdms2
import cwt
import zmq
from celery.utils.log import get_task_logger

from wps import settings
from wps.tasks import process

logger = get_task_logger('wps.tasks.edas')

def check_exceptions(data):
    if '<exceptions>' in data:
        index = data.index('!')

        data = data[index+1:]

        root = ET.fromstring(data)

        exceptions = root.findall('./exceptions/*')

        if len(exceptions) > 0:
            raise Exception(exceptions[0].text)

def initialize_socket(context, socket_type, host, port):
    sock = context.socket(socket_type)

    sock.connect('tcp://{}:{}'.format(host, port))

    return sock

def listen_edas_output(self, poller, job):
    edas_output_path = None

    self.status(job, 'Listing for EDAS status')

    while True:
        events = dict(poller.poll(settings.EDAS_TIMEOUT * 1000))

        if len(events) == 0:
            raise Exception('EDAS timed out waiting for heartbeat or output message')

        data = events.keys()[0].recv()

        check_exceptions(data)

        parts = data.split('!')

        if 'file' in parts:
            sub_parts = parts[-1].split('|')

            edas_output_path = sub_parts[-1]

            break
        elif 'response' in parts:
            self.status(job, 'EDAS heartbeat')
        
    self.status(job, 'Received success from EDAS backend')

    return edas_output_path

@process.cwt_shared_task()
def edas_submit(self, parent_variables, variables, domains, operation, **kwargs):
    self.PUBLISH = process.ALL

    req_sock = None

    sub_sock = None

    edas_output_path = None

    user, job = self.initialize(credentials=False, **kwargs)

    v, d, o = self.load(parent_variables, variables, domains, operation)

    domain = d.get(o.domain, None)

    o.inputs = v.values()

    data_inputs = cwt.WPS('').prepare_data_inputs(o, {}, domain)

    logger.info('Generated datainputs: {}'.format(data_inputs))

    context = zmq.Context.instance()

    try:
        req_sock = initialize_socket(context, zmq.REQ, settings.EDAS_HOST, settings.EDAS_REQ_PORT)

        sub_sock = initialize_socket(context, zmq.SUB, settings.EDAS_HOST, settings.EDAS_RES_PORT)

        sub_sock.setsockopt(zmq.SUBSCRIBE, b'{}'.format(job.id))

        poller = zmq.Poller()

        poller.register(sub_sock)

        self.status(job, 'Connected to EDAS backend')

        extra = '{"response":"file"}'

        req_sock.send(str('{}!execute!{}!{}!{}'.format(job.id, o.identifier, data_inputs, extra)))

        if (req_sock.poll(settings.EDAS_TIMEOUT * 1000) == 0):
            raise Exception('EDAS timed out waiting for initial response')

        data = req_sock.recv()

        self.status(job, 'Sent request to EDAS backend')

        check_exceptions(data)

        edas_output_path = listen_edas_output(self, poller, job)

        if edas_output_path is None:
            raise Exception('Failed to receive output from EDAS')
    except:
        raise
    finally:
        if req_sock is not None:
            req_sock.close()

        if sub_sock is not None:
            poller.unregister(sub_sock)

            sub_sock.close()

    self.status(job, 'Received result from EDAS backend')
            
    output_path = self.generate_output_path()

    shutil.move(edas_output_path, output_path)

    self.status(job, 'Localizing output to THREDDS server')

    output_url = self.generate_output_url(output_path)

    var_name = None

    try:
        with cdms2.open(output_path) as infile:
            var_name = infile.variables.keys()[0]
    except:
        raise Exception('Error with accessing EDAS output file')

    self.status(job, 'Variable name from EDAS result "{}"'.format(var_name))

    output_var = cwt.Variable(output_url, var_name, name=o.name)

    return {o.name, output_var.parameterize()}
