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
        if len(operations) == 0:
            raise Exception('Must atlease supply one operation')

        logger.info('Executing process "{}"'.format(identifier))

        params = {
            'user_id': kwargs.get('user').id,
            'job_id': kwargs.get('job').id
        }

        operation = operations.values()[0]

        domain = operation.domain

        variable_dict = dict((x, variables[x].parameterize()) for x in operation.inputs)

        domain_dict = {domain: domains[domain].parameterize()}

        logger.info('Variables {}'.format(variable_dict))

        logger.info('Domains {}'.format(domain_dict))

        logger.info('Operation {}'.format(operation))

        cache_task = tasks.cache_variable.si({}, variable_dict, domain_dict, operation.parameterize(), **params)

        operation.inputs = [operation.name]

        oph_task = tasks.oph_submit.s({}, domain_dict, operation.parameterize(), **params)

        (cache_task | oph_task).delay()
