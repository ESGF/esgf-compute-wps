import json
import logging

import celery
import cwt

from wps import helpers
from wps import tasks
from wps.backends import backend
from wps.tasks import base

__ALL__ = ['Local']

logger = logging.getLogger('wps.backends')

class Local(backend.Backend):
    def initialize(self):
        pass

    def populate_processes(self):
        logger.info('Registering processes for backend "local"')

        for name, proc in base.REGISTRY.iteritems():
            self.add_process(name, name.title(), proc.ABSTRACT)

    def ingress(self, chunk_map_raw, operation, user_id, job_id):
        chunk_map = json.loads(chunk_map_raw, object_hook=helpers.json_loads_object_hook)

        base_units = chunk_map['base_units']

        del chunk_map['base_units']

        process = cwt.Process.from_dict(json.loads(operation))

        ingress_tasks = []

        for uri, chunk_list in chunk_map.iteritems():
            for chunk in chunk_list:
                ingress_tasks.append(tasks.ingress.s(uri, chunk, ''))

        try:
            aggregate = base.REGISTRY['CDAT.aggregate']
        except KeyError:
            raise base.WPSError('Something went wrong, missing aggregate process')

        aggregate_sig = aggregate.s({}, {}, operation, user_id, job_id)

        return celery.chord(ingress_tasks)(aggregate_sig)

    def execute(self, identifier, variables, domains, operations, **kwargs):
        if len(operations) == 0:
            raise Exception('Must supply atleast one operation')

        operation = operations.values()[0].parameterize()

        variable_dict = dict((x, y.parameterize()) for x, y in variables.iteritems())

        domain_dict = dict((x, y.parameterize()) for x, y in domains.iteritems())

        target_process = base.get_process(identifier)

        logger.info('Retrieved process "{}"'.format(identifier))

        job = kwargs.get('job')

        user = kwargs.get('user')

        params = {
            'job_id': job.id,
            'user_id': user.id,
        }

        logger.info('Variables {}'.format(variable_dict))

        logger.info('Domains {}'.format(domain_dict))

        logger.info('Operation {}'.format(operation))

        return tasks.preprocess.s({}, variable_dict, domain_dict, operation, **params)
        #return target_process.s({}, variable_dict, domain_dict, operation, **params)

    def get_task(self, identifier):
        try:
            task = base.get_process(identifier)
        except:
            if 'CDSpark' in identifier:
                task = tasks.edas_submit
            elif 'Oph' in identifier:
                task = tasks.oph_submit

        return task

    def workflow(self, root_op, variables, domains, operations, **kwargs):
        job = kwargs.get('job')

        user = kwargs.get('user')

        params = {
            'job_id': job.id,
            'user_id': user.id,
        }

        global_domains = dict((x, y.parameterize()) for x, y in domains.iteritems())

        logger.info('Building workflow')

        def _build(node):
            sub_tasks = []

            for name in node.inputs:
                if name in operations:
                    sub_tasks.append(_build(operations[name]))

            task_variables = dict((x, variables[x].parameterize()) 
                                  for x in node.inputs if x in variables)

            if len(sub_tasks) == 0:
                task = self.get_task(node.identifier).s(
                    {}, task_variables, global_domains, node.parameterize(), **params)
            else:
                task = self.get_task(node.identifier).s(
                    task_variables, global_domains, node.parameterize(), **params)

                task = celery.group(sub_tasks) | task

            return task

        return _build(root_op)
