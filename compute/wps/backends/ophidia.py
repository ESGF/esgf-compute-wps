import logging

from wps import tasks
from wps import settings
from wps.backends import backend
from wps.tasks.ophidia import PROCESSES

logger = logging.getLogger('wps.backends.ophdia')

__all__ = ['Ophidia']

class Ophidia(backend.Backend):
    def initialize(self):
        pass

    def populate_processes(self):
        logger.info('Registering processes for backend "ophidia"')

        for key in PROCESSES.keys():
            self.add_process(key, key)

    def execute(self, identifier, variables, domains, operations, **kwargs):
        logger.info('Executing process "{}"'.format(identifier))

        params = {
            'cwd': '/tmp',
            'user_id': kwargs.get('user').id,
            'job_id': kwargs.get('job').id
        }

        chain = tasks.cache_variable.si(identifier, variables, domains, operations, **params)

        chain = chain | tasks.oph_submit.s(identifier, **params)

        chain.delay()
