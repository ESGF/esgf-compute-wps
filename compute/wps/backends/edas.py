import logging
import uuid
import xml.etree.ElementTree as ET

import cwt
import zmq
from celery.task.control import inspect
from celery.task.control import revoke
from django.conf import settings

from wps import models
from wps import tasks
from wps import WPSError
from wps.backends import backend
from wps.tasks import helpers

__ALL__ = ['EDAS']

logger = logging.getLogger('wps.backends')

class EDASCommunicationError(WPSError):
    def __init__(self, hostname, port):
        msg = 'Communication error with EDAS backend at {hostname}:{port}'

        super(EDASCommunicationError, self).__init__(msg, hostname=hostname, port=port)

class EDAS(backend.Backend):
    def initialize(self):
        pass

    def parse_get_capabilities(self, data):
        index = data.index('!')

        header = data[:index]

        body = data[index+1:]

        capabilities = {}

        root = ET.fromstring(body)

        for module in root:
            module_name = module.attrib['name']

            logger.info('Processing module %r', module_name)

            for kernel in module:
                name = kernel.attrib['name']

                logger.info('\tProcessing kernel %r', name)

                id = '{}:{}'.format(module_name, name)

                #describe = self.describe_process(id)

                title = kernel.attrib['title']

                capabilities[name] = {
                    'module': module_name,
                    'title': title,
                }

        return capabilities

    def get_capabilities(self):
        context = zmq.Context.instance()

        socket = context.socket(zmq.REQ)

        socket.connect('tcp://{}:{}'.format(settings.WPS_EDAS_HOST, settings.WPS_EDAS_REQ_PORT))

        id = uuid.uuid4()

        socket.send('{}!getCapabilities'.format(id))

        # Poll so we can timeout eventually
        if (socket.poll(10 * 1000) == 0):
            logger.info('Failed to retrieve EDAS response')

            socket.close()

            raise EDASCommunicationError(settings.WPS_EDAS_HOST, settings.WPS_EDAS_REQ_PORT)

        data = socket.recv()

        socket.close()

        response = self.parse_get_capabilities(data)

        return response

    def describe_process(self, identifier):
        context = zmq.Context.instance()

        socket = context.socket(zmq.REQ)

        socket.connect('tcp://{}:{}'.format(settings.WPS_EDAS_HOST, settings.WPS_EDAS_REQ_PORT))

        id = uuid.uuid4()

        request = '{}!describeprocess!{}'.format(id, identifier)

        logger.info('Sending %r', request)

        socket.send(request)

        # Poll so we can timeout eventually
        if (socket.poll(10 * 1000) == 0):
            logger.info('Failed to retrieve EDAS response')

            socket.close()

            raise EDASCommunicationError(settings.WPS_EDAS_HOST, settings.WPS_EDAS_REQ_PORT)

        data = socket.recv()

    def populate_processes(self):
        server = models.Server.objects.get(host='default')

        logger.info('Registering processes for backend "edas"')

        response = self.get_capabilities()

        metadata = {'inputs': '*', 'datasets': '*'}

        for x, y in response.iteritems():
            identifier = 'EDASK.{}-{}'.format(y['module'], x)

            self.add_process(identifier, y['title'], metadata,
                             abstract=y['title'])

    def execute(self, identifier, variable, domain, operation, **kwargs):
        logger.debug('%r', kwargs)

        logger.info('Executing process "{}"'.format(identifier))

        params = {
            'user_id': kwargs.get('user').id,
            'job_id': kwargs.get('job').id
        }

        variable, domain, operation = self.load_data_inputs(variable, domain, operation)

        operation = operation.values()[0]

        start = tasks.job_started.s(job_id=params['job_id']).set(**helpers.DEFAULT_QUEUE)

        submit = tasks.edas_submit.si(variable.values(), operation.domain, operation, **params).set(**helpers.DEFAULT_QUEUE)

        canvas = start | submit

        canvas.delay()
