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

class FileNotIncludedError(WPSError):
    pass

class CDAT(backend.Backend):
    def initialize(self):
        pass

    def populate_processes(self):
        logger.info('Registering processes for backend "local"')

        for name, proc in base.REGISTRY.iteritems():
            self.add_process(
                name, name.title(), proc.METADATA, data_inputs=proc.INPUT, 
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

    def configure_preprocess(self, variable, domain, operation, user, job, **kwargs):
        logger.info('Configuring preprocess')

        variable, domain, operation = self.load_data_inputs(variable, domain, operation)

        # Eventually workflow preprocessing will be handlded
        if len(operation.values()) > 1:
            raise WPSError('Workflows are unsupported')

        op = operation.values()[0]

        inputs = [x for x in op.inputs if isinstance(x, cwt.Variable)]

        SINGLE_INPUT = 'CDAT\.(subset|regrid|sum|min|max|average)'
        REQUIRE_AXES = 'CDAT\.(min|max|sum|average)'

        if re.match(SINGLE_INPUT, op.identifier) is not None:
            inputs = sorted(inputs, key=lambda x: x.uri)[0:1]

        if re.match(REQUIRE_AXES, op.identifier) is not None:
            axis_param = op.get_parameter('axes')

            if axis_param is None:
                raise WPSError('Missing required parameter "axes"')

            axis = axis_param.values
        else:
            axis = None

        try:
            time_dimen = op.domain.get_dimension('time')
        except AttributeError:
            time_dimen = None

        var_name = set(x.var_name for x in inputs).pop()

        uris = [x.uri for x in inputs]

        logger.info('operation %r', op)
        logger.info('domain %r', op.domain)
        logger.info('axis %r', axis)
        logger.info('var_name %r', var_name)
        logger.info('uris %r', uris)

        start = tasks.start_job.s(job_id=job.id).set(
            **helpers.INGRESS_QUEUE)

        base = tasks.determine_base_units.si(
            uris, var_name, user.id, job_id=job.id).set(
                **helpers.INGRESS_QUEUE)

        canvas = start | base

        analysis = {}

        for uri in uris:
            check_cache = tasks.check_cache.s(
                uri, var_name, job_id=job.id).set(**helpers.DEFAULT_QUEUE)

            generate_chunks = tasks.generate_chunks.s(
                uri, axis, job_id=job.id).set(**helpers.DEFAULT_QUEUE)

            analysis[uri] = [check_cache, generate_chunks]

        if time_dimen is not None and time_dimen.crs.name == cwt.INDICES.name:
            map_domain = tasks.map_domain_time_indices.s(
                var_name, op.domain, user.id, job_id=job.id).set(
                    **helpers.INGRESS_QUEUE)

            canvas = canvas | map_domain
        else:

            for uri in uris:
                map_domain = tasks.map_domain.s(
                    uri, var_name, op.domain, user.id, job_id=job.id).set(
                        **helpers.INGRESS_QUEUE)

                analysis[uri].insert(0, map_domain)

            canvas = canvas | celery.group(celery.chain(x) for x in analysis.values())

        execute = tasks.wps_execute.s(
            variable, domain, operation, user_id=user.id, job_id=job.id).set(
                **helpers.DEFAULT_QUEUE)

        canvas = canvas | execute

        canvas.delay()

    def generate_ingress_tasks(self, op_uid, url, var_name, user_id, job_id, **kwargs):
        preprocess = kwargs[url]

        chunks = preprocess['chunks']

        chunk_axis = chunks.keys()[0]

        chunk_list = chunks.values()[0]

        mapped = preprocess['mapped']

        if mapped is None:
            raise FileNotIncludedError()

        ingress_paths = []
        ingress_tasks = []

        for chunk in chunk_list:
            key = '{}-{:08}'.format(op_uid, kwargs['index'])

            kwargs['index'] += 1

            ingress_paths.append('{}/{}.nc'.format(
                settings.WPS_INGRESS_PATH, key))

            mapped_copy = mapped.copy()

            mapped_copy.update({chunk_axis: chunk})

            ingress_tasks.append(
                tasks.ingress_uri.s(
                    url, var_name, mapped_copy, ingress_paths[-1], user_id, 
                    job_id=job_id).set(
                        **helpers.INGRESS_QUEUE))

        return ingress_paths, ingress_tasks

    def generate_cache_entry(self, op_uid, url, cache_files, **kwargs):
        key = '{}-{:08}'.format(op_uid, kwargs['index'])

        kwargs['index'] += 1

        preprocess = kwargs[url]

        cached = preprocess['cached']

        chunks = preprocess['chunks']

        cache_entry = {'cached': True}

        cache_entry.update(cached)

        cache_entry['chunk_axis'] = chunks.keys()[0]

        cache_entry['chunk_list'] = chunks.values()[0]

        cache_files[key] = cache_entry

    def execute_processing(self, root, sort, base_units, variable, domain, operation, user_id, job_id, **kwargs):
        op = operation[root]

        logger.info('Executing %r', op)

        op_uid = uuid.uuid4()

        # Should produce a single value
        var_name = set([x.var_name for x in variable.values()]).pop()

        # Sort the inputs so we generate the ingress chunks in order
        urls = sorted([variable[x].uri for x in op.inputs], key=lambda x: kwargs[x][sort])

        ingress = []
        cache = []
        cache_files = {}
        cleanup_paths = []

        kwargs['index'] = 0

        for url in urls:
            logger.info('Processing %r', url)

            preprocess = kwargs[url]

            cached = preprocess['cached']

            mapped = preprocess['mapped']

            if cached is None:
                chunks = preprocess['chunks']

                chunk_axis = chunks.keys()[0]

                try:
                    ingress_paths, ingress = self.generate_ingress_tasks(op_uid, url, var_name, user_id, job_id, **kwargs)
                except FileNotIncludedError:
                    continue

                cleanup_paths.extend(ingress_paths)

                cache.append(tasks.ingress_cache.s(
                    url, var_name, mapped, chunk_axis, base_units, 
                    job_id=job_id).set(
                        **helpers.DEFAULT_QUEUE))
            else:
                self.generate_cache_entry(op_uid, url, cache_files, **kwargs)

        del kwargs['index']

        output_path = '{}\{}.nc'.format(settings.WPS_LOCAL_OUTPUT_PATH, op_uid)

        success = tasks.job_succeeded.s(
            output_path, None, var_name, job_id=job_id).set(
                **helpers.DEFAULT_QUEUE)

        process = base.get_process(op.identifier)

        if len(ingress) > 0:
            process_task = process.s(
                ingress_paths, op, var_name, base_units, job_id=job_id).set(
                    **helpers.DEFAULT_QUEUE)

            ingress_and_process = celery.group(x for x in ingress) | process_task

            cleanup = tasks.cleanup.s(cleanup_paths, job_id=job_id).set(
                **helpers.DEFAULT_QUEUE)

            finalize = celery.group(x for x in cache) | cleanup

            canvas = ingress_and_process | success | finalize
        else:
            process_task = process.s(
                cache_files, cache_files.keys(), op, var_name, base_units, 
                job_id=job_id).set(
                    **helpers.DEFAULT_QUEUE)

            canvas = process_task | success

        canvas.delay()

    def execute_computation(self, root, base_units, variable, domain, operation, user_id, job_id, **kwargs):
        op = operation[root]

        logger.info('Executing %r', op)

        op_uid = uuid.uuid4()

        axes = op.get_parameter('axes')

        var = variable[op.inputs[0]]

        preprocess = kwargs[var.uri]

        cached = preprocess['cached']

        chunks = preprocess['chunks']

        chunk_axis = chunks.keys()[0]

        ingress = []
        cache_files = {}
        output_paths = []
        cleanup_paths = []

        kwargs['index'] = 0

        process = base.get_process(op.identifier)

        if cached is None:
            ingress_paths, ingress = self.generate_ingress_tasks(op_uid, var.uri, var.var_name, user_id, job_id, **kwargs)

            cleanup_paths.extend(ingress_paths)

            mapped = preprocess['mapped'].copy()

            cache = tasks.ingress_cache.s(
                var.uri, var.var_name, mapped, chunk_axis, base_units, job_id=job_id).set(
                    **helpers.DEFAULT_QUEUE)

            index = 0
            axes_sig = '-'.join(axes.values)
            process_chains = []

            for ingress_path, ingress_task in zip(ingress_paths, ingress):
                output_paths.append('{}/{}-{:08}-{}.nc'.format(
                    settings.WPS_INGRESS_PATH, op_uid, index, axes_sig))

                index += 1 

                process_chains.append(
                    celery.chain(ingress_task, process.s(
                        ingress_path, op, var.var_name, base_units, 
                        axes.values, output_paths[-1], job_id=job_id).set(
                            **helpers.DEFAULT_QUEUE)))

            cleanup_paths.extend(output_paths)
        else:
            self.generate_cache_entry(op_uid, var.uri, cache_files, **kwargs)

            cached = cache_files.values()[0]

            chunk_axis = cached['chunk_axis']

            chunk_list = cached['chunk_list']

            index = 0
            axes_sig = '-'.join(axes.values)
            process_chains = []

            for chunk in chunk_list:
                output_paths.append('{}/{}-{:08}-{}.nc'.format(
                    settings.WPS_INGRESS_PATH, op_uid, index, axes_sig))

                index += 1

                mapped = preprocess['mapped'].copy()

                mapped.update({chunk_axis: chunk})

                data = {
                    var.uri: {
                        'cached': True,
                        'path': cached['path'],
                        'mapped': mapped,
                    }
                }

                process_chains.append(process.s(
                    data, var.uri, op, var.var_name, base_units, axes.values, 
                    output_paths[-1], job_id=job_id).set(
                        **helpers.DEFAULT_QUEUE))

            cleanup_paths.extend(output_paths)

        del kwargs['index']

        concat_path = '{}/{}-concat'.format(settings.WPS_INGRESS_PATH, op_uid)

        concat = tasks.concat_process_output.s(
            output_paths, var.var_name, chunk_axis, concat_path, 
            job_id=job_id).set(
                **helpers.DEFAULT_QUEUE)

        output_path = '{}/{}.nc'.format(settings.WPS_LOCAL_OUTPUT_PATH, op_uid)

        success = tasks.job_succeeded.s(
            concat_path, output_path, var.var_name, job_id=job_id).set(
                **helpers.DEFAULT_QUEUE)

        canvas = celery.group(x for x in process_chains) | concat | success

        if len(ingress) > 0:
            canvas = canvas | cache

        cleanup = tasks.cleanup.s(cleanup_paths, job_id=job_id).set(
            **helpers.DEFAULT_QUEUE)

        canvas = canvas | cleanup

        canvas.delay()

    def execute_simple(self, identifier, user, job, process, **kwargs):
        process_task = base.get_process(identifier)

        canvas = process_task.s(user_id=user.id, job_id=job.id, process_id=process.id, **kwargs)

        canvas = canvas.set(**helpers.DEFAULT_QUEUE)

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
            identifier = kwargs['identifier']

            process = base.get_process(identifier)

            metadata = process.METADATA

            if 'inputs' in metadata and metadata['inputs'] == 0:
                self.execute_simple(**kwargs)
            else:
                self.configure_preprocess(**kwargs)
