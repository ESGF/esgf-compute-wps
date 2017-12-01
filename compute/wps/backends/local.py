import logging

import cwt

from celery import chord
from celery import group
from celery import signature
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
            self.add_process(name, name.title(), proc.abstract)

    def execute(self, identifier, variables, domains, operations, **kwargs):
        if len(operations) == 0:
            raise Exception('Must supply atleast one operation')

        operation = operations.values()[0].parameterize()

        variable_dict = dict((x, y.parameterize()) for x, y in variables.iteritems())

        domain_dict = dict((x, y.parameterize()) for x, y in domains.iteritems())

        target_process = process.get_process(identifier)

        logger.info('Retrieved process "{}"'.format(identifier))

        job = kwargs.get('job')

        user = kwargs.get('user')

        params = {
            'job_id': job.id,
            'user_id': user.id,
        }

        logger.info('Building task chain')

        chain = target_process.s({}, variable_dict, domain_dict, operation, **params)

        logger.info('Executing task chain')

        chain.delay()

    def get_task(self, identifier):
        try:
            task = process.get_process(identifier)
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

                task = group(sub_tasks) | task

            return task

        workflow = _build(root_op)

        workflow.delay()
