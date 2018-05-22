import json
import logging
import os

import celery
import cwt
from django.conf import settings

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

    def ingress(self, chunk_map, domains, operation, **kwargs):
        logger.info('Configuring ingress')

        user = kwargs.get('user')

        job = kwargs.get('job')

        process = kwargs.get('process')

        domains = dict((x, y.parameterize()) for x, y in domains.iteritems())

        index = 0
        ingress_tasks = []
        ingress_map = {}

        logger.info('Mapping ingressed files to source files')

        for uri, meta in chunk_map.iteritems():
            ingress_map[uri] = {
                'temporal': meta['temporal'],
                'spatial': meta['spatial'],
                'base_units': meta['base_units'],
                'variable_name': meta['variable_name'],
                'ingress_chunks': []
            }

            logger.info('Processing ingressed files for %s', uri)

            for local_index, chunk in enumerate(meta['chunks']):
                output_filename = 'ingress-{}-{:04}.nc'.format('some-uid', index)

                index += 1

                output_uri = os.path.join(settings.WPS_INGRESS_PATH, output_filename)

                ingress_map[uri]['ingress_chunks'].append(output_uri)

                chunk_data = json.dumps(chunk, default=helpers.json_dumps_default)

                ingress_tasks.append(tasks.ingress.s(uri, meta['variable_name'], chunk_data, meta['base_units'], output_uri))

        ingress_map = json.dumps(ingress_map, default=helpers.json_dumps_default)

        logger.info('Putting together task pipeline')

        if operation.identifier not in ('CDAT.aggregate', 'CDAT.subset'):
            ingress_cache_sig = tasks.ingress_cache.s(ingress_map, job_id=job.id)

            process = base.REGISTRY[operation.identifier]

            params = {
                'user_id': user.id,
                'job_id': job.id,
                'process_id': process.id,
            }

            process_sig = process.s({}, domains, operation.parameterize(), **params)

            process_sig = process_sig.set(queue='priority.low', exchange='priority', routing_key='low')

            canvas = celery.chain(celery.group(ingress_tasks), ingress_cache_sig, process_sig)
        else:
            ingress_cache_sig = tasks.ingress_cache.s(ingress_map, job_id=job.id, process_id=process.id)

            canvas = celery.chain(celery.group(ingress_tasks), ingress_cache_sig)

        return canvas

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

        process = kwargs.get('process')

        params = {
            'job_id': job.id,
            'user_id': user.id,
            'process_id': process.id,
        }

        target_process = target_process.s({}, variable_dict, domain_dict, operation, **params)

        target_process = target_process.set(queue='priority.low', exchange='priority', routing_key='low')

        return target_process

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
