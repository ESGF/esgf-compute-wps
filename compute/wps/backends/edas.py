import logging
import xml.etree.ElementTree as ET

import cwt
import zmq
from celery.task.control import inspect
from celery.task.control import revoke

from wps import models
from wps import settings
from wps import tasks
from wps import wps_xml
from wps.backends import backend

__ALL__ = ['EDAS']

logger = logging.getLogger('wps.backends')

class EDAS(backend.Backend):
    def initialize(self):
        pass

    def populate_processes(self):
        server = models.Server.objects.get(host='default')

        logger.info('Registering processes for backend "edas"')

        context = zmq.Context.instance()

        socket = context.socket(zmq.REQ)

        socket.connect('tcp://{}:{}'.format(settings.EDAS_HOST, settings.EDAS_REQ_PORT))

        socket.send(str('0!getCapabilities!WPS'))

        # Poll so we can timeout eventually
        if (socket.poll(10 * 1000) == 0):
            logger.info('Falied to retrieve EDAS response')

            socket.close()

            return

        data = socket.recv()

        socket.close()

        header, response = data.split('!')

        root = ET.fromstring(str(response))

        tags = root.findall('./processes/*')

        for tag in tags:
            desc_tag = tag.find('description')

            identifier = desc_tag.attrib.get('id')

            title = desc_tag.attrib.get('title')

            abstract = desc_tag.text

            self.add_process(identifier, title, abstract)

    def execute(self, identifier, variables, domains, operations, **kwargs):
        if len(operations) == 0:
            raise Exception('Must provide atleast one operation')

        logger.info('Executing process "{}"'.format(identifier))

        params = {
            'user_id': kwargs.get('user').id,
            'job_id': kwargs.get('job').id
        }

        operation = operations.values()[0]

        domain = operation.domain

        variable_dict = dict((x, variables[x].parameterize()) for x in operation.inputs)

        domain_dict = dict(domain = domains[operation.domain].parameterize())

        cache_op = operation.parameterize()

        cache_task = tasks.cache_variable.si({}, variable_dict, domain_dict, cache_op, **params)

        operation.inputs = [operation.name]

        edas_task = tasks.edas_submit.s({}, domain_dict, operation.parameterize(), **params)

        (cache_task | edas_task).delay()
