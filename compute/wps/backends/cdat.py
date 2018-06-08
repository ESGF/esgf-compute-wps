import json
import logging
import os
import uuid

import celery
import cwt
import numpy as np
from django.conf import settings

from wps import helpers
from wps import models
from wps import tasks
from wps.backends import backend
from wps.tasks import base

__ALL__ = ['CDAT']

logger = logging.getLogger('wps.backends')

class CDAT(backend.Backend):
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

        estimate_size = kwargs.get('estimate_size')

        domains = dict((x, y.parameterize()) for x, y in domains.iteritems())

        index = 0
        ingress_tasks = []
        ingress_map = {}

        logger.info('Mapping ingressed files to source files')

        ingress_uid = uuid.uuid4()

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
                output_filename = 'ingress-{}-{:04}.nc'.format(ingress_uuid, index)

                index += 1

                output_uri = os.path.join(settings.WPS_INGRESS_PATH, output_filename)

                ingress_map[uri]['ingress_chunks'].append(output_uri)

                chunk_data = json.dumps(chunk, default=helpers.json_dumps_default)

                ingress_tasks.append(tasks.ingress.s(uri, meta['variable_name'], chunk_data, meta['base_units'], output_uri))

        ingress_map = json.dumps(ingress_map, default=helpers.json_dumps_default)

        logger.info('Putting together task pipeline')

        queue = helpers.determine_queue(process, float(estimate_size))

        logger.info('Routing to queue %r', queue)

        if operation.identifier not in ('CDAT.aggregate', 'CDAT.subset'):
            process = base.REGISTRY[operation.identifier]

            new_kwargs = {
                'user_id': user.id,
                'job_id': job.id,
                'process_id': process.id,
            }

            ingress_cache_sig = tasks.ingress_cache.s(ingress_map, job_id=job.id)

            ingress_cache_sig = ingress_cache_sig.set(**queue)

            process_sig = process.s({}, domains, operation.parameterize(), **new_kwargs)

            process_sig = process_sig.set(**queue)

            canvas = celery.chain(celery.group(ingress_tasks), ingress_cache_sig, process_sig)
        else:
            new_kwargs = {
                'job_id': job.id,
                'process_id': process.id,
            }

            ingress_cache_sig = tasks.ingress_cache.s(ingress_map, **new_kwargs)

            ingress_cache_sig = ingress_cache_sig.set(**queue)

            canvas = celery.chain(celery.group(ingress_tasks), ingress_cache_sig)

        return canvas

    def execute(self, identifier, variables, domains, operations, **kwargs):
        job = kwargs.get('job')

        user = kwargs.get('user')

        process = kwargs.get('process')

        estimate_size = kwargs.get('estimate_size')

        operation = operations.values()[0].parameterize()

        variable_dict = dict((x, y.parameterize()) for x, y in variables.iteritems())

        domain_dict = dict((x, y.parameterize()) for x, y in domains.iteritems())

        target_process = base.get_process(identifier)

        logger.info('Retrieved process "{}"'.format(identifier))

        new_kwargs = {
            'job_id': job.id,
            'user_id': user.id,
            'process_id': process.id,
            'domain_map': kwargs.get('domain_map'),
        }

        target_process = target_process.s({}, variable_dict, domain_dict, operation, **new_kwargs)

        queue = helpers.determine_queue(process, float(estimate_size))

        logger.info('Routing to queue %r', queue)

        target_process = target_process.set(**queue)

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

        preprocess = kwargs.get('preprocess', {})

        domains = dict((x, y.parameterize()) for x, y in domains.iteritems())

        logger.info('Building workflow with root %r', root_op)

        queue = {
            'queue': 'priority.low',
            'exchange': 'priority',
            'routing_key': 'low',
        }

        def _build(node):
            logger.info('Processing node %r', node)

            node_preprocess = preprocess.get(node.name)

            logger.info('Node preprocess %r', node_preprocess)

            sub_tasks = []

            for name in node.inputs:
                if name in operations:
                    sub_tasks.append(_build(operations[name]))

            logger.info('Subtasks %r', len(sub_tasks))

            task_variables = dict((x, variables[x].parameterize()) 
                                  for x in node.inputs if x in variables)

            logger.info('Variables %r', len(task_variables))

            try:
                process = models.Process.objects.get(identifier=node.identifier)
            except models.Process.DoesNotExist:
                raise base.WPSError('Error finding process {name}', name=node.identifier)

            args = [
                task_variables, 
                domains, 
            ]

            new_kwargs = {
                'user_id': user.id,
                'job_id': job.id,
                'process_id': process.id,
            }

            ingress_canvas = None

            if node_preprocess is not None:
                if node_preprocess['type'] == 'ingress':
                    chunk_map = json.loads(node_preprocess['data'], object_hook=helpers.json_loads_object_hook)

                    ingress_canvas, output_name = self.build_ingress_canvas(chunk_map, queue, node, user, job, process)
                else:
                    new_kwargs['domain_map'] = node_preprocess['data']

            if ingress_canvas is None:
                if len(sub_tasks) == 0:
                    args.insert(0, {})

                args.append(node.parameterize())

                task = self.get_task(node.identifier).s(*args, **new_kwargs)

                task.set(**queue)
            else:
                if node.identifier in ('CDAT.subset', 'CDAT.aggregate'):
                    task = ingress_canvas
                else:
                    if len(sub_tasks) == 0:
                        args.insert(0, {})

                    node.inputs = [output_name,]

                    args.append(node.parameterize())

                    task = self.get_task(node.identifier).s(*args, **new_kwargs)

                    task.set(**queue)

                    task = celery.chain(ingress_canvas, task)

            logger.info('Created task with %r and %r', args, new_kwargs)

            if len(sub_tasks) > 0:
                task = celery.chain(celery.group(sub_tasks), task)

            return task

        return _build(root_op)

    def build_ingress_canvas(self, chunk_map, queue, operation, user, job, process):
        index = 0
        ingress_tasks = []
        ingress_map = {}

        logger.info('Mapping ingressed files to source files')

        ingress_uuid = uuid.uuid4()

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
                output_filename = 'ingress-{}-{:04}.nc'.format(ingress_uuid, index)

                index += 1

                output_uri = os.path.join(settings.WPS_INGRESS_PATH, output_filename)

                ingress_map[uri]['ingress_chunks'].append(output_uri)

                chunk_data = json.dumps(chunk, default=helpers.json_dumps_default)

                ingress_tasks.append(tasks.ingress.s(uri, meta['variable_name'], chunk_data, meta['base_units'], output_uri))

        ingress_map = json.dumps(ingress_map, default=helpers.json_dumps_default)

        logger.info('Putting together task pipeline')

        new_kwargs = {
            'job_id': job.id,
            'process_id': process.id,
            'output_id': operation.name,
        }

        ingress_cache_sig = tasks.ingress_cache.s(ingress_map, **new_kwargs)

        ingress_cache_sig = ingress_cache_sig.set(**queue)

        canvas = celery.chain(celery.group(ingress_tasks), ingress_cache_sig)

        return canvas, operation.name
