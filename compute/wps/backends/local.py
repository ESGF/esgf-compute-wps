import logging

import cwt

from wps import models
from wps import wps_xml
from wps import tasks
from wps.processes import REGISTRY
from wps.processes import get_process
from wps.backends import backend

__ALL__ = ['Local']

logger = logging.getLogger('wps.backends')

class Local(backend.Backend):
    def initialize(self):
        pass

    def populate_processes(self):
        try:
            server = models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            raise Exception('Default server does not exist')

        logger.info('Registering processes for backend "local"')

        for name, proc in REGISTRY.iteritems():
            logger.info('Registering "{}" process'.format(name))

            desc = wps_xml.describe_process_response(name, name.title(), '')

            try:
                proc = models.Process.objects.get(identifier=name, backend='Local')
            except models.Process.DoesNotExist:
                proc = models.Process.objects.create(identifier=name, backend='Local', description=desc.xml())

                proc.server_set.add(server)
            else:
                proc.description = desc.xml()

                proc.save()

    def execute(self, identifier, variables, domains, operations, **kwargs):
        process = get_process(identifier)

        logger.info('Retrieved process "{}"'.format(identifier))

        job = kwargs.get('job')

        user = kwargs.get('user')

        params = {
            'cwd': '/tmp',
            'job_id': job.id,
            'user_id': user.id
        }

        logger.info('Building task chain')

        chain = tasks.check_auth.s(**params)

        chain = (chain | process.si(variable_dict, operation_dict, domain_dict, **params))

        logger.info('Executing task chain')

        chain()
