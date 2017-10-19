import logging

import cwt

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
        logger.info('Registering processes for backend "local"')

        for name, proc in REGISTRY.iteritems():
            logger.info('Registering "{}" process'.format(name))

            self.add_process(name, name.title(), 'Local')

    def execute(self, identifier, variables, domains, operations, **kwargs):
        process = get_process(identifier)

        logger.info('Retrieved process "{}"'.format(identifier))

        job = kwargs.get('job')

        user = kwargs.get('user')

        params = {
            'job_id': job.id,
            'user_id': user.id
        }

        logger.info('Building task chain')

        chain = process.si(variables, operations, domains, **params)

        logger.info('Executing task chain')

        chain.delay()
