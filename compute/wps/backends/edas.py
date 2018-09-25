import logging
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

    def get_capabilities(self):
        context = zmq.Context.instance()

        socket = context.socket(zmq.REQ)

        socket.connect('tcp://{}:{}'.format(settings.WPS_EDAS_HOST, settings.WPS_EDAS_REQ_PORT))

        socket.send(str('0!getCapabilities!WPS'))

        # Poll so we can timeout eventually
        if (socket.poll(10 * 1000) == 0):
            logger.info('Failed to retrieve EDAS response')

            socket.close()

            raise EDASCommunicationError(settings.WPS_EDAS_HOST, settings.WPS_EDAS_REQ_PORT)

        data = socket.recv()

        socket.close()

        header, response = data.split('!')

        return response

    def populate_processes(self):
        server = models.Server.objects.get(host='default')

        logger.info('Registering processes for backend "edas"')

        response = self.get_capabilities()

        root = ET.fromstring(str(response))

        tags = root.findall('./processes/*')

        metadata = {'inputs': '*', 'datasets': '*'}

        for tag in tags:
            desc_tag = tag.find('description')

            identifier = desc_tag.attrib.get('id')

            title = desc_tag.attrib.get('title')

            abstract = desc_tag.text

            self.add_process(identifier, title, metadata, abstract=abstract)

    def execute(self, identifier, variable, domain, operation, **kwargs):
        logger.debug('%r', kwargs)

        logger.info('Executing process "{}"'.format(identifier))

        params = {
            'user_id': kwargs.get('user').id,
            'job_id': kwargs.get('job').id
        }

        variable, domain, operation = self.load_data_inputs(variable, domain, operation)

        operation = operation.values()[0]

        canvas = tasks.edas_submit.si(variable.values(), operation.domain, operation, **params).set(**helpers.DEFAULT_QUEUE)

        canvas.delay()
