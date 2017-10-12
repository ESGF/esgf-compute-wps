import logging
import xml.etree.ElementTree as ET

import cwt
import zmq
from celery.task.control import inspect
from celery.task.control import revoke

from wps import models
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

        socket.connect('tcp://{}:{}'.format('edas', 5670))

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

            logger.info('Registering process "{}"'.format(identifier))

            desc = wps_xml.describe_process_response(identifier, title, abstract)

            try:
                proc = models.Process.objects.get(identifier=identifier, backend=self.name)
            except models.Process.DoesNotExist:
                proc = models.Process.objects.create(identifier=identifier, backend=self.name, description=desc.xml())

                proc.server_set.add(server)
            else:
                proc.description = desc.xml()

                proc.save()

    def execute(self, identifier, variables, domains, operations, **kwargs):
        logger.info('Executing process "{}"'.format(identifier))

        params = {
            'cwd': '/tmp',
            'user_id': kwargs.get('user').id,
            'job_id': kwargs.get('job').id
        }

        chain = tasks.check_auth.s(**params)

        chain = chain | tasks.cache_variable.si(identifier, variables, domains, operations, **params)

        chain = chain | tasks.edas_submit.s(identifier, **params)

        chain()
