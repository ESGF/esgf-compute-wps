import logging

import cwt

from wps import tasks
from wps.backends import backend
from wps.tasks import process

__ALL__ = ['Local']

logger = logging.getLogger('wps.backends')

class Local(backend.Backend):
    def initialize(self):
        pass

    def populate_processes(self):
        logger.info('Registering processes for backend "local"')

        for name, proc in process.REGISTRY.iteritems():
            self.add_process(name, name.title())

    def execute(self, identifier, variables, domains, operations, **kwargs):
        target_process = process.get_process(identifier)

        logger.info('Retrieved process "{}"'.format(identifier))

        job = kwargs.get('job')

        user = kwargs.get('user')

        params = {
            'job_id': job.id,
            'user_id': user.id
        }

        logger.info('Building task chain')

        chain = target_process.si(variables, operations, domains, **params)

        logger.info('Executing task chain')

        chain.delay()
