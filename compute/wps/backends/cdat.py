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
from wps.context import OperationContext
from wps.tasks import base

logger = logging.getLogger('wps.backends')

# These operations require only a single input
SINGLE_INPUT = 'CDAT\.(subset|regrid|sum|min|max|average)'

# These operations require axes parameter
REQUIRE_AXES = 'CDAT\.(min|max|sum|average)'

class FileNotIncludedError(WPSError):
    pass

class CDAT(backend.Backend):
    def initialize(self):
        pass

    def populate_processes(self):
        logger.info('Registering processes for backend "local"')

        for name, proc in base.REGISTRY.iteritems():
            self.add_process(name, name.title(), metadata=proc.METADATA,
                             data_inputs=proc.DATA_INPUTS,
                             process_outputs=proc.PROCESS_OUTPUTS,
                             abstract=proc.ABSTRACT, hidden=proc.HIDDEN)
            
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

    def extract_configuration(self, variable, domain, operation):
        """ Extracts the configuration.

        Extracts the required configuration items from the CWT WPS execute
        request.

        Args:
            variable: A dict keyed with the names of the cwt.Variables
            objects.
            domain: A dict keyed with the names of the cwt.Domain obejcts.
            operation: A dict keyed with the name of the cwt.Process objects.

        Returns:
            A dict with the following items:

                operation: A cwt.Process object which is the root operation.
                axis: A list of str axes if provided.
                time_axis: A cwt.Dimension object for the time axis in the
                domain.
                var_name: A str variable name.
                uris: A list of str URIs.
        """
        data = {}

        if len(operation.values()) > 1:
            raise WPSError('Workflows are unsupported')

        data['operation'] = operation.values()[0]

        inputs = [x for x in data['operation'].inputs if isinstance(x, cwt.Variable)]

        if re.match(SINGLE_INPUT, data['operation'].identifier) is not None:
            inputs = sorted(inputs, key=lambda x: x.uri)[0:1]

        if re.match(REQUIRE_AXES, data['operation'].identifier) is not None:
            axis_param = data['operation'].get_parameter('axes')

            if axis_param is None:
                raise WPSError('Missing required parameter "axes"')

            data['axis'] = axis_param.values
        else:
            data['axis'] = None

        try:
            data['time_axis'] = data['operation'].domain.get_dimension('time')
        except AttributeError:
            data['time_axis'] = None

        data['var_name'] = set(x.var_name for x in inputs).pop()

        data['uris'] = [x.uri for x in inputs]

        return data

    def configure_preprocess(self, variable, domain, operation, user, job, **kwargs):
        """ Configures the preprocess workflow.

        Args:
            variable: A dict keyed with the names of cwt.Variable objects.
            domain: A dict keyed with the names of cwt.Domain objects.
            operation: A dict keyed with the names of cwt.Process objects.
            user: A wps.models.User object.
            job: A wps.models.Job object.
            **kwargs: Misc named args.

        Returns:
            None
        """
        logger.info('Configuring preprocess')

        variable, domain, operation = self.load_data_inputs(variable, domain, operation)

        config = self.extract_configuration(variable, domain, operation)

        start = tasks.job_started.s(job_id=job.id).set(
            **helpers.INGRESS_QUEUE)

        base = tasks.determine_base_units.si(
            config['uris'], config['var_name'], user.id, job_id=job.id).set(
                **helpers.INGRESS_QUEUE)

        # Start tracking steps
        job.steps_inc_total(2)

        # Start of the chain will mark the job started and get the base units
        # of all files.
        canvas = start | base

        analysis = {}

        # For each file we'll check the cache and generate the chunks over the
        # termporal axis.
        for uri in config['uris']:
            check_cache = tasks.check_cache.s(
                uri, config['var_name'], job_id=job.id).set(**helpers.DEFAULT_QUEUE)

            generate_chunks = tasks.generate_chunks.s(
                config['operation'], uri, config['axis'], job_id=job.id).set(**helpers.DEFAULT_QUEUE)

            analysis[uri] = [check_cache, generate_chunks]

        # The way the domain is mapped depends on some criteria. If the time
        # axis is given and is of CRS type indices then we need to generate the
        # mapped domain for each file in serial, since the current depends on
        # the previous. If the time axis is not given or is of CRS type values
        # then the mapped domain can be compute in parallel for each file.
        if config['time_axis'] is not None and config['time_axis'].crs.name == cwt.INDICES.name:
            map_domain = tasks.map_domain_time_indices.s(
                config['var_name'], config['operation'].domain, user.id, job_id=job.id).set(
                    **helpers.INGRESS_QUEUE)

            canvas = canvas | map_domain

            job.steps_inc_total()
        else:
            for uri in config['uris']:
                map_domain = tasks.map_domain.s(
                    uri, config['var_name'], config['operation'].domain, user.id, job_id=job.id).set(
                        **helpers.INGRESS_QUEUE)

                analysis[uri].insert(0, map_domain)

        canvas = canvas | celery.group(celery.chain(x) for x in analysis.values())

        job.steps_inc_total(len(analysis) * len(analysis.values()[0]))

        # Setup the task to submit the job for actual execution.
        execute = tasks.wps_execute.s(
            variable, domain, operation, user_id=user.id, job_id=job.id).set(
                **helpers.DEFAULT_QUEUE)

        job.steps_inc_total()

        canvas = canvas | execute

        canvas.delay()

    def generate_ingress_tasks(self, uid, var, user_id, job_id,
                               state, chunks, mapped, **kwargs):
        chunk_axis = chunks.keys()[0]

        chunk_list = chunks.values()[0]

        if mapped is None:
            raise FileNotIncludedError()

        chunk_length = len(chunk_list)

        chunks_per_task = int(max(round(chunk_length/settings.WORKER_PER_USER),
                                 1.0))

        logger.info('Total chunks %r, chunks per task %r', chunk_length,
                    chunks_per_task)

        chunk_groups = [chunk_list[x:x+chunks_per_task] for x in xrange(0,
                                                                 chunk_length,
                                                                 chunks_per_task)]

        ingresses = []

        for group in chunk_groups:
            group_maps = {}

            for chunk in group:
                key = '{}-{:08}'.format(uid, state['index'])

                state['index'] += 1

                ingress_path = '{}/{}.nc'.format(settings.WPS_INGRESS_PATH, key)

                mapped_copy = mapped.copy()
                
                mapped_copy.update({chunk_axis: chunk})

                group_maps[ingress_path] = mapped_copy

            ingress_task = tasks.ingress_uri.s(var.uri, var.var_name,
                                               group_maps, user_id,
                                               job_id=job_id).set(**helpers.INGRESS_QUEUE)

            ingresses.append((group_maps.keys(), ingress_task))

        return ingresses

    def generate_cache_entry(self, uid, state, cached, chunks, **kwargs):
        key = '{}-{:08}'.format(uid, state['index'])

        state['index'] += 1

        cache_entry = {'cached': True}

        cache_entry.update(cached)

        cache_entry['chunk_axis'] = chunks.keys()[0]

        cache_entry['chunk_list'] = chunks.values()[0]

        state['cache_files'][key] = cache_entry

    def configure_processing(self, op, base_units, user, job, sort, **kwargs):
        var_name = None

        ingresses = []
        cache_tasks = []

        state = {'index': 0, 'cache_files': {}}

        variable = sorted([self.get_variable(x, **kwargs) for x in op.inputs],
                          key=lambda x: kwargs[x.uri][sort])

        for var in variable:
            if var_name is None:
                var_name = var.var_name

            preprocess = kwargs[var.uri]

            if preprocess['mapped'] is None:
                continue

            if preprocess['cached'] is None:
                ingress = self.generate_ingress_tasks(op.name, var, user.id,
                                                      job.id, state,
                                                      **preprocess)

                ingresses.extend(ingress)

                chunk_axis = preprocess['chunks'].keys()[0]

                cache_tasks.append(tasks.ingress_cache.s(var.uri, var.var_name,
                                                         preprocess['mapped'].copy(),
                                                         chunk_axis,
                                                         base_units,
                                                         job_id=job.id).set(**helpers.DEFAULT_QUEUE))

                job.steps_inc_total(len(ingress)+1)
            else:
                self.generate_cache_entry(op.name, state, **preprocess)

        return {
            'var_name': var_name, 
            'ingresses': ingresses,
            'cache_tasks': cache_tasks,
            'cache_files': state['cache_files'],
        }

    def execute_processing(self, user_id, job_id, base_units, **kwargs):
        job = self.load_job(job_id)

        job.steps_reset()

        job.update('Starting processing')

        user = self.load_user(user_id)

        op = self.get_operation(**kwargs)

        process = models.Process.objects.get(identifier=op.identifier)

        config = self.configure_processing(op, base_units, user, job, **kwargs)

        output_path = self.generate_output_path(user, job, op.name)

        success = tasks.job_succeeded.s(
            kwargs['variable'].values(), output_path, None, config['var_name'],
            process.id, user.id,job_id=job.id).set(**helpers.DEFAULT_QUEUE)

        job.steps_inc_total()

        process = base.get_process(op.identifier)

        if len(config['ingresses']) > 0:
            logger.info('Processing from ingress')

            ingress_paths = [y for x in config['ingresses'] for y in x[0]]

            ingress_tasks = [x[1] for x in config['ingresses']]

            process_task = process.s(ingress_paths, op, config['var_name'],
                                     base_units, output_path,
                                     job_id=job.id).set(**helpers.DEFAULT_QUEUE)

            ingress_and_process = celery.chord(header=ingress_tasks,
                                               body=process_task)

            cleanup = tasks.ingress_cleanup.s(ingress_paths, job_id=job.id).set(
                **helpers.DEFAULT_QUEUE)

            finalize = celery.group(x for x in config['cache_tasks']) | cleanup

            canvas = ingress_and_process | success | finalize

            job.steps_inc_total(3)
        else:
            logger.info('Processing from cache')

            process_task = process.s(config['cache_files'],
                                     config['cache_files'].keys(), op,
                                     config['var_name'],
                                     base_units, output_path,
                                     job_id=job.id).set(**helpers.DEFAULT_QUEUE)

            canvas = process_task | success

            job.steps_inc_total(2)

        canvas.delay()

    def get_operation(self, root, operation, **kwargs):
        return operation[root]

    def get_variable(self, name, variable, **kwargs):
        return variable[name]

    def configure_ingress_computation(self, var, op, user, job, base_units, mapped, chunks, **kwargs):
        state = {'index': 0}

        axes = op.get_parameter('axes')

        process = base.get_process(op.identifier)

        chunk_axis = chunks.keys()[0]

        ingress = self.generate_ingress_tasks(op.name, var, user.id,
                                              job.id, state, chunks, mapped)

        cache = tasks.ingress_cache.s(var.uri, var.var_name, mapped.copy(),
                                      chunk_axis, base_units,
                                      job_id=job.id).set(**helpers.DEFAULT_QUEUE)

        index = 0
        axes_sig = '-'.join(axes.values)
        temp_paths = []
        process_paths = []
        process_chains = []

        for paths, task in ingress:
            temp_paths.extend(paths)

            filename = '{}-{:08}-{}.nc'.format(op.name, index, axes_sig)

            index += 1

            process_paths.append(os.path.join(settings.WPS_INGRESS_PATH,
                                              filename))
            
            process_chains.append(celery.chain(task, process.s(paths, op,
                                                               var.var_name,
                                                               base_units,
                                                               chunk_axis,
                                                               axes.values,
                                                               process_paths[-1],
                                                               job_id=job.id).set(
                                                                   **helpers.DEFAULT_QUEUE)))

        job.steps_inc_total((len(process_chains)*2)+1)

        return {
            'temp_paths': temp_paths,
            'cache': cache,
            'chunk_axis': chunk_axis,
            'process_paths': process_paths,
            'process_chains': process_chains,
        }

    def configure_cached_computation(self, var, op, user, job, base_units, **kwargs):
        logger.info('Configuring computation from cache')

        state = {'index': 0, 'cache_files': {}}

        axes = op.get_parameter('axes')

        logger.info('Computing over %r', axes)

        process = base.get_process(op.identifier)

        self.generate_cache_entry(op.name, state, **kwargs)

        cached = state['cache_files'].values()[0]

        chunk_axis = cached['chunk_axis']

        chunk_list = cached['chunk_list']

        chunk_length = len(chunk_list)


        chunks_per_task = int(max(round(chunk_length/settings.WORKER_PER_USER),
                                 1.0))

        logger.info('Total chunks %r, chunks per task %r', chunk_length,
                    chunks_per_task)

        chunk_groups = [chunk_list[x:x+chunks_per_task] for x in
                        xrange(0, chunk_length, chunks_per_task)]

        index = 0
        axes_sig = '-'.join(axes.values)
        process_paths = []
        process_chains = []

        for group in chunk_groups:
            keys = []
            attrs = {}

            filename = '{}-{:08}-{}.nc'.format(op.name, index, axes_sig)

            index += 1

            process_paths.append(os.path.join(settings.WPS_INGRESS_PATH, filename))

            for i, chunk in enumerate(group):
                mapped = cached['mapped'].copy()

                mapped.update({chunk_axis: chunk})

                keys.append(str(i))

                attrs[keys[-1]] = {
                    'path': cached['path'],
                    'mapped': mapped,
                }

            process_chains.append(process.s(attrs, keys, op, var.var_name,
                                            base_units, chunk_axis,
                                            axes.values, process_paths[-1],
                                            job_id=job.id).set(**helpers.DEFAULT_QUEUE))

        job.steps_inc_total(len(process_chains))

        return {
            'temp_paths': [],
            'chunk_axis': cached['chunk_axis'],
            'process_paths': process_paths,
            'process_chains': process_chains,
        }

    def configure_computation(self, user, job, base_units, **kwargs):
        op = self.get_operation(**kwargs)

        var = self.get_variable(op.inputs[0], **kwargs)

        preprocess = kwargs[var.uri]

        if preprocess['cached'] is None:
            config = self.configure_ingress_computation(var, op, user, job,
                                                        base_units,
                                                        **preprocess)
        else:
            config = self.configure_cached_computation(var, op, user, job,
                                                       base_units,
                                                       **preprocess)

        return config

    def execute_computation(self, user_id, job_id, **kwargs):
        job = self.load_job(job_id)

        job.steps_reset()

        job.update('Starting computation')

        user = self.load_user(user_id)

        op = self.get_operation(**kwargs)

        var = self.get_variable(op.inputs[0], **kwargs)

        config = self.configure_computation(user, job, **kwargs)

        output_path = self.generate_output_path(user, job, op.name)

        process_obj = models.Process.objects.get(identifier=op.identifier)

        extra_cleanup_paths = []

        if len(config['process_chains']) > 1:
            concat_path = '{}/{}-concat'.format(settings.WPS_INGRESS_PATH,
                                                op.name)

            extra_cleanup_paths.append(concat_path)

            concat = tasks.concat_process_output.s(config['process_paths'],
                                                   op, var.var_name,
                                                   config['chunk_axis'],
                                                   concat_path,
                                                   job_id=job.id).set(**helpers.DEFAULT_QUEUE)

            success = tasks.job_succeeded.s([var], concat_path, output_path, var.var_name,
                                            process_obj.id, user.id,
                                            job_id=job.id).set(**helpers.DEFAULT_QUEUE)

            logger.info('Executing multiple process chains')

            canvas = celery.group(x for x in config['process_chains']) | concat | success
        else:
            success = tasks.job_succeeded.s([var], config['process_paths'][0],
                                            output_path, var.var_name,
                                            process_obj.id, user.id,
                                            job_id=job.id).set(**helpers.DEFAULT_QUEUE)

            logger.info('Executing single process chain')

            canvas = config['process_chains'][0] | success

        if 'cache' in config:
            canvas = canvas | config['cache']

        cleanup = tasks.ingress_cleanup.s(config['temp_paths'] +
                                          config['process_paths'] +
                                          extra_cleanup_paths,
                                          job_id=job.id).set(**helpers.DEFAULT_QUEUE)

        canvas = canvas | cleanup

        job.steps_inc_total(3)

        canvas.delay()

    def execute_simple(self, identifier, user, job, process, **kwargs):
        process_task = base.get_process(identifier)

        canvas = process_task.s(user_id=user.id, job_id=job.id, process_id=process.id, **kwargs)

        canvas = canvas.set(**helpers.DEFAULT_QUEUE)

        canvas.delay()

    def execute_workflow(self, identifier, variable, domain, operation, user,
                         job, process, **kwargs):
        process_task = base.get_process(identifier)

        variable, domain, operation = self.load_data_inputs(variable, domain,
                                                            operation)

        start = tasks.job_started.s(job_id=job.id).set(**helpers.DEFAULT_QUEUE)

        workflow = process_task.si(variable, domain, operation, user_id=user.id,
                                job_id=job.id,
                                 **kwargs).set(**helpers.DEFAULT_QUEUE)

        var_name = set(x.var_name for x in variable.values()).pop()

        succeeded = tasks.job_succeeded_workflow.s(variable.values(),
                                                   process.id, user.id,
                                                   job_id=job.id).set(**helpers.DEFAULT_QUEUE)

        canvas = start | workflow | succeeded;

        canvas.delay()

    def execute(self, identifier, variable, domain, operation, process, job,
                user):
        if identifier == 'CDAT.workflow':
            pass
        else:
            context = OperationContext.from_data_inputs(identifier, variable,
                                                        domain, operation)

            context.job = job

            context.user = user

            context.process = process

            preprocess_chains = []

            for index in range(settings.WORKER_PER_USER):
                filter = tasks.filter_inputs.s(index).set(**helpers.DEFAULT_QUEUE)

                map = tasks.map_domain.s().set(**helpers.DEFAULT_QUEUE)

                cache = tasks.check_cache.s().set(**helpers.DEFAULT_QUEUE)

                chunks = tasks.generate_chunks.s().set(**helpers.DEFAULT_QUEUE)

                preprocess_chains.append(celery.chain(filter, map, cache,
                                                      chunks))

            start = tasks.job_started.s(context).set(**helpers.DEFAULT_QUEUE)

            units = tasks.base_units.s().set(**helpers.DEFAULT_QUEUE)

            merge = tasks.merge.s().set(**helpers.DEFAULT_QUEUE)

            preprocess = start | units | celery.group(preprocess_chains) | merge

            process_func = base.get_process(identifier)

            process_chains = []

            for index in range(settings.WORKER_PER_USER):
                ingress = tasks.ingress_chunk.s(index).set(**helpers.DEFAULT_QUEUE)

                process = process_func.s(index).set(**helpers.DEFAULT_QUEUE)

                process_chains.append(celery.chain(ingress, process))

            concat = tasks.concat.s().set(**helpers.DEFAULT_QUEUE)

            success = tasks.job_succeeded.s().set(**helpers.DEFAULT_QUEUE)

            cache = tasks.ingress_cache.s().set(**helpers.DEFAULT_QUEUE)

            cleanup = tasks.ingress_cleanup.s().set(**helpers.DEFAULT_QUEUE)

            finalize = concat | success | cache | cleanup

            canvas = preprocess | celery.group(process_chains) | finalize

            canvas.delay()
