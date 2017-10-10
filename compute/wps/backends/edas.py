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

def get_task(name):
    i = inspect().active()

    return [y for x in i.values() for y in x if y['name'] == name]

class EDAS(backend.Backend):
    def initialize(self):
        logger.info('Initializing EDAS backend')

        existing_tasks = get_task('wps.tasks.edas_listen')

        for task in existing_tasks:
            logger.info('Killing EDAS listener task "{}"'.format(task['id']))

            revoke(task['id'], terminate=True)

        logger.info('Starting EDAS listener task')

        tasks.edas_listen.delay('edas', '5671')

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

    def execute(self, identifier, data_inputs, **kwargs):
        job = kwargs.get('job')

        # CDAS doesn't understand timestamps, so we'll need to rewrite them
        operations, domains, variables = cwt.WPS.parse_data_inputs(data_inputs)

        operations[0].inputs = []

        data_inputs = cwt.WPS('').prepare_data_inputs(operations[0], variables, None)

        job.started()

        context = zmq.Context.instance()

        socket = context.socket(zmq.REQ)

        socket.connect('tcp://{}:{}'.format('edas', 5670))

        extra = '{"response":"file"}'

        request = '{}!execute!{}!{}!{}'.format(0, identifier, data_inputs, extra)

        socket.send(request)

        socket.close()
