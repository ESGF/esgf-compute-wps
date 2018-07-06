import json
import logging
import os
import re
import uuid

import celery
import cwt
import numpy as np
from django.conf import settings

from wps import helpers
from wps import models
from wps import tasks
from wps import WPSError
from wps.backends import backend
from wps.tasks import base

__ALL__ = ['CDAT']

logger = logging.getLogger('wps.backends')

PROCESSING_OP = 'CDAT\.(subset|aggregate|regrid)'

class CDAT(backend.Backend):
    def initialize(self):
        pass

    def populate_processes(self):
        logger.info('Registering processes for backend "local"')

        for name, proc in base.REGISTRY.iteritems():
            self.add_process(name, name.title(), data_inputs=proc.INPUT, 
                             process_outputs=proc.OUTPUT, abstract=proc.ABSTRACT)

    def load_data_inputs(self, variable_raw, domain_raw, operation_raw):
        variable = {}

        for item in json.loads(variable_raw):
            v = cwt.Variable.from_dict(item)

            variable[v.name] = v

        logger.info('Loaded %r variables', len(variable))

        domain = {}

        for item in json.loads(domain_raw):
            d = cwt.Domain.from_dict(item)

            domain[d.name] = d

        logger.info('Loaded %r domains', len(domain))

        operation = {}

        for item in json.loads(operation_raw):
            o = cwt.Process.from_dict(item)

            operation[o.name] = o

        logger.info('Loaded %r operations', len(operation))

        for o in operation.values():
            if o.domain is not None:
                logger.info('Resolving domain %r', o.domain)

                o.domain = domain[o.domain]

            inputs = []

            for inp in o.inputs:
                if inp in variable:
                    inputs.append(variable[inp])
                elif inp in operation:
                    inputs.append(operation[inp])
                else:
                    kwargs = {
                        'inp': inp,
                        'name': o.name,
                        'id': o.identifier,
                    }

                    raise WPSError('Unabled to resolve input "{inp}" for operation "{name}" - "{id}"', **kwargs)

                logger.info('Resolved input %r', inp)

            o.inputs = inputs

        return variable, domain, operation

    def configure_preprocess(self, identifier, variable, domain, operation, user, job, **kwargs):
        logger.info('Configuring preprocess workflow')

        variable, domain, operation = self.load_data_inputs(variable, domain, operation)

        dom = dict((x.name, x) for x in domain.values())

        op = dict((x.name, x) for x in operation.values())

        var = dict((x.name, x) for x in variable.values())

        variables = []

        for operation in op.values():
            inp = [x for x in operation.inputs if isinstance(x, cwt.Variable)]

            if re.match('CDAT\.(subset|regrid|sum|min|max|average)', operation.identifier) is not None:
                inp = sorted(inp, key=lambda x: x.uri)[0:1]

            if re.match('CDAT\.(min|max|sum|average)', operation.identifier) is not None:
                axis_param = operation.get_parameter('axes')

                if axis_param is None:
                    raise WPSError('Missing required parameter "axes"')

                axis = axis_param.values
            else:
                axis = None

            operation.inputs = [x.name for x in inp]

            var_name = set(x.var_name for x in inp).pop()

            uris = [x.uri for x in inp]

            base = tasks.determine_base_units.s(
                uris, var_name, user.id, job_id=job.id).set(
                    **helpers.DEFAULT_QUEUE)

            variables.append(
                (base | 
                 celery.group([
                     (tasks.map_domain.s(
                         x.uri, var_name, operation.domain, user.id, 
                         job_id=job.id).set(
                             **helpers.DEFAULT_QUEUE) |
                      tasks.check_cache.s(
                          x.uri, job_id=job.id).set(
                              **helpers.DEFAULT_QUEUE) |
                      tasks.generate_chunks.s(
                          x.uri, axis, job_id=job.id).set(
                              **helpers.DEFAULT_QUEUE)) for x in inp])))

        analyze = tasks.analyze_wps_request.s(
            var, dom, op, user.id, job_id=job.id).set(
                **helpers.DEFAULT_QUEUE)

        request = tasks.request_execute.s(job_id=job.id).set(**helpers.DEFAULT_QUEUE)

        validate = tasks.validate.s(job_id=job.id).set(**helpers.DEFAULT_QUEUE)

        if len(variables) == 1:
            canvas = (variables[0] | analyze | validate | request)
        else:
            #canvas = (celery.group(variables) | analyze | validate | request)
            # Need to fix this, celery isn't liking groups in the header of a 
            # chord, possible solution is launch a canvas for each group of 
            # variables in an operation and have a task poll for completion 
            # manually.
            raise WPSError('Failed to run preprocessing on multiple variables')

        canvas.delay()

    def execute_processing(self, root, var_name, user_id, job_id, **kwargs):
        logger.info('Configure processing')

        root_op = kwargs['operation'][root]

        logger.info('Executing process %r', root_op)

        op_uuid = uuid.uuid4()

        index = 0

        ingress_task_list = []
        cache_task_list = []
        cache_list = []

        for inp in root_op.inputs:
            var = kwargs['variable'][inp]

            logger.info('Processing %r', var)

            cached = kwargs['cached'][var.uri]

            mapped = kwargs['mapped'][var.uri]

            if mapped is None:
                logger.info('Skipping %r, has an empty map', var.uri)

                continue

            file_base_units = kwargs['units'][var.uri]

            chunked_axis = kwargs['chunks'][var.uri].keys()[0]

            chunks = kwargs['chunks'][var.uri][chunked_axis]

            if cached is None:
                logger.info('%r is not cached', var.name)

                orig_mapped = mapped.copy()

                for chunk in chunks:
                    key = '{0}-{1:06}'.format(op_uuid, index)

                    filename = '{}.nc'.format(key)

                    index += 1

                    ingress_output = os.path.join(settings.WPS_INGRESS_PATH, filename)

                    mapped.update({ chunked_axis: chunk })

                    ingress_task_list.append(tasks.ingress_uri.s(
                        key, var.uri, var_name, mapped.copy(), ingress_output, 
                        user_id, job_id=job_id).set(
                            **helpers.DEFAULT_QUEUE))

                cache_task_list.append(tasks.ingress_cache.s(
                    var.uri, var_name, orig_mapped, file_base_units, 
                    job_id=job_id).set(
                        **helpers.DEFAULT_QUEUE))
            else:
                logger.info('%r is cached', var.name)

                uri = cached['path']

                mapped = cached['mapped']

                key = '{0}-{1:06}'.format(op_uuid, index)

                index += 1

                cache_list.append({
                    'key': key,
                    'cached': {
                        'path': uri,
                        'chunked_axis': chunked_axis,
                        'chunks': chunks,
                        'mapped': mapped.copy(),
                    }
                })

        process = base.get_process(operation.identifier)

        if len(ingress_task_list) > 0:
            canvas = celery.group(ingress_task_list)
        else:
            canvas = None

        if canvas is None:
            canvas = process.s({}, cache_list, root_op, var_name, base_units, 
                               job_id=job_id).set(
                                   **helpers.DEFAULT_QUEUE)
        else:
            canvas = (canvas |
                      process.s(cache_list, root_op, var_name, base_units,
                                job_id=job_id).set(
                                    **helpers.DEFAULT_QUEUE) |
                      celery.group(cache_task_list) |
                      tasks.ingress_cleanup.s(job_id=job_id).set(
                          **helpers.DEFAULT_QUEUE))

        canvas.delay()

    def execute_computation(self, root, variable, operation, var_name, base_units, mapped, chunks, cached, user_id, job_id, **kwargs):
        logger.info('Configure computation')
        
        logger.info('%s', json.dumps(kwargs, indent=4, default=helpers.json_dumps_default))

        root_op = operation[root]

        process = base.get_process(root_op.identifier)

        axes = root_op.get_parameter('axes')

        var = variable[root_op.inputs[0]]

        var_mapped = mapped[var.uri]

        orig_mapped = var_mapped.copy()

        var_chunks = chunks[var.uri]

        var_cached = cached[var.uri]

        chunked_axis = var_chunks.keys()[0]

        chunks = var_chunks[chunked_axis]

        index = 0

        op_uuid = uuid.uuid4()

        ingress_task_list = []
        process_task_list = []

        if var_cached is None:
            for chunk in chunks:
                key = '{0}-{1:06}'.format(op_uuid, index)

                filename = '{}.nc'.format(key)

                process_filename = '{}-{}.nc'.format(key, axes.values[0])

                index += 1

                ingress_output = os.path.join(settings.WPS_INGRESS_PATH, filename)

                process_output = os.path.join(settings.WPS_INGRESS_PATH, process_filename)

                var_mapped.update({ chunked_axis: chunk })

                ingress_task_list.append(
                    tasks.ingress_uri.s(
                        key, var.uri, var_name, var_mapped.copy(), 
                        ingress_output, user_id, job_id=job_id).set(
                            **helpers.DEFAULT_QUEUE) |
                     process.s(root_op, var_name, base_units, axes.values[0:1],
                               process_output, job_id=job_id).set(
                         **helpers.DEFAULT_QUEUE))
        else:
            var_mapped = cached[var.uri]['mapped']

            for chunk in chunks:
                key = '{0}-{1:06}'.format(op_uuid, index)

                filename = '{}.nc'.format(key)

                process_filename = '{}-{}.nc'.format(key, axes.values[0])

                index += 1

                ingress_output = os.path.join(settings.WPS_INGRESS_PATH, filename)

                process_output = os.path.join(settings.WPS_INGRESS_PATH, process_filename)

                var_mapped.update({ chunked_axis: chunk })

                attrs = {
                    key: {
                        'cached': {
                            'path': cached[var.uri]['path'],
                            'domain': var_mapped.copy(),
                        }
                    }
                }

                process_task_list.append(
                    process.s(attrs, root_op, var_name, base_units, axes.values[0:1],
                              process_output, job_id=job_id).set(
                                  **helpers.DEFAULT_QUEUE))

        if len(ingress_task_list) > 0:
            canvas = (celery.group(ingress_task_list) | 
                      tasks.concat_process_output.s(var_name, job_id=job_id).set(
                          **helpers.DEFAULT_QUEUE) |
                      tasks.ingress_cache.s(var.uri, var_name, orig_mapped, 
                                            base_units, job_id=job_id).set(
                                                **helpers.DEFAULT_QUEUE) |
                      tasks.ingress_cleanup.s(job_id=job_id).set(
                          **helpers.DEFAULT_QUEUE))
        else:
            canvas = (celery.group(process_task_list) |
                      tasks.concat_process_output.s(var_name, job_id=job_id).set(
                          **helpers.DEFAULT_QUEUE))

        canvas.delay()

    def execute(self, **kwargs):
        if 'preprocess' in kwargs:
            root = kwargs['root']

            root_op = kwargs['operation'][root]

            if kwargs['workflow']:
                raise WPSError('Workflows have been disabled')
            else:
                if re.match(PROCESSING_OP, root_op.identifier) is not None:
                    self.execute_processing(**kwargs)
                else:
                    self.execute_computation(**kwargs)
        else:
            self.configure_preprocess(**kwargs)
