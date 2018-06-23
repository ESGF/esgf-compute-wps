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

CHUNKED_TIME = ('CDAT.subset', 'CDAT.aggregate', 'CDAT.regrid')

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

    def operation_task(self, operation, var_dict, dom_dict, op_dict, user, job):
        inp = [x for x in operation.inputs if isinstance(x, cwt.Variable)]

        # TODO this will eventually be removed once we start restricting the number
        # of allowed inputs, subet and regrid expect a single input
        if operation.identifier in ('CDAT.subset', 'CDAT.regrid'):
            inp = sorted(inp, key=lambda x: x.uri)[0:1]

        var_name = set(x.var_name for x in inp).pop()

        uris = [x.uri for x in inp]

        base = tasks.determine_base_units.s(
            uris, var_name, user.id).set(
                **helpers.DEFAULT_QUEUE)

        variables = celery.group(
            (tasks.map_domain.s(
                x.uri, var_name, operation.domain, user.id).set(
                    **helpers.DEFAULT_QUEUE) |
             tasks.check_cache.s(
                 x.uri).set(
                     **helpers.DEFAULT_QUEUE) |
             tasks.generate_chunks.s(
                 x.uri, 'time').set(
                     **helpers.DEFAULT_QUEUE)) for x in inp)

        analyze = tasks.analyze_wps_request.s(
            var_dict, dom_dict, op_dict, user.id, job.id).set(
                **helpers.DEFAULT_QUEUE)

        request = tasks.request_execute.s().set(**helpers.DEFAULT_QUEUE)

        return (base | variables | analyze | request)

    def configure_preprocess(self, **kwargs):
        logger.info('Configuring preprocess workflow')

        identifier = kwargs['identifier']

        variable = kwargs['variable']

        domain = kwargs['domain']

        operation = kwargs['operation']

        user = kwargs['user']

        job = kwargs['job']

        variable, domain, operation = self.load_data_inputs(variable, domain, operation)

        operations = []

        dom = dict((x.name, x) for x in domain.values())

        op = dict((x.name, x) for x in operation.values())

        var = dict((x.name, x) for x in variable.values())

        for item in operation.values():
            operations.append(self.operation_task(item, var, dom, op, user, job))

        canvas = celery.group(operations)

        canvas.delay()

    def configure_execute(self, **kwargs):
        logger.info('Configuring execute')

        root = kwargs['root']

        root_op = kwargs['operation'][root]

        variable = kwargs['variable']

        var_name = kwargs['var_name']

        base_units = kwargs['base_units']

        user_id = kwargs['user_id']

        job_id = kwargs['job_id']

        if root_op.identifier in CHUNKED_TIME:
            chunk_axis = 'time'
        else:
            chunk_axis = None

        variable_list = []

        index = 0
        op_uuid = uuid.uuid4()

        for inp in root_op.inputs:
            var = variable[inp]

            var_meta = kwargs[var.uri]

            uri = var.uri if kwargs.get('cached') is None else kwargs['cached']

            chunks = var_meta.get('chunks')

            mapped = var_meta['mapped']

            if chunks is None:
                continue

            axis = chunks.keys()[0]

            chunks = chunks[axis]

            for chunk in chunks:
                mapped.update({ axis: chunk })

                filename = '{}:{}.nc'.format(op_uuid, index)

                file_path = os.path.join(settings.WPS_INGRESS_PATH)

                args = [uri, var_name, mapped, file_path, user_id, job_id,
                        base_units if chunk_axis == 'time' else None]

                variable_list.append(task.ingress.s(*args).set(
                        **helpers.DEFAULT_QUEUE))

                index += 1

        return celery.group(variable_list)

    def execute(self, **kwargs):
        if 'preprocess' in kwargs:
            if kwargs['workflow']:
                raise WPSError('Workflows have been disabled')
            else:
                self.configure_execute(**kwargs)
        else:
            self.configure_preprocess(**kwargs)
