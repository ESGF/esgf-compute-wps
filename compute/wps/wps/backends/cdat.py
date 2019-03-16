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
from wps.context import WorkflowOperationContext
from wps.tasks import base

logger = logging.getLogger('wps.backends')

class CDAT(backend.Backend):
    def populate_processes(self):
        logger.info('Registering processes for backend "local"')

        for name, proc in base.REGISTRY.iteritems():
            self.add_process(name, metadata=proc.METADATA, abstract=proc.ABSTRACT)

    def execute_workflow(self, identifier, context):
        process_task = base.get_process(identifier)

        start = tasks.job_started.s(context).set(**helpers.DEFAULT_QUEUE)

        workflow = process_task.s().set(**helpers.DEFAULT_QUEUE)

        succeeded = tasks.job_succeeded_workflow.s().set(**helpers.DEFAULT_QUEUE)

        context.job.step_inc(2 + len(context.operation))

        canvas = start | workflow | succeeded;

        canvas.delay()

    def execute_simple(self, context, process_func):
        start = tasks.job_started.s(context).set(**helpers.DEFAULT_QUEUE)

        process = process_func.s().set(**helpers.DEFAULT_QUEUE)

        success = tasks.job_succeeded.s().set(**helpers.DEFAULT_QUEUE)

        context.job.step_inc(3)

        canvas = start | process | success

        canvas.delay()

    def generate_preprocess_chains(self, context):
        preprocess_chains = []
        tasks_total = 0

        for index in range(settings.WORKER_PER_USER):
            filter = tasks.filter_inputs.s(index).set(**helpers.DEFAULT_QUEUE)

            map = tasks.map_domain.s().set(**helpers.DEFAULT_QUEUE)

            cache = tasks.check_cache.s().set(**helpers.DEFAULT_QUEUE)

            chunks = tasks.generate_chunks.s().set(**helpers.DEFAULT_QUEUE)

            preprocess_chains.append(celery.chain(filter, map, cache,
                                                  chunks))

            tasks_total += 3

        context.job.step_inc(tasks_total)

        return preprocess_chains

    def generate_process_chains(self, context, process_func=None):
        process_chains = []
        tasks_total = 0

        for index in range(settings.WORKER_PER_USER):
            chain = []

            if settings.INGRESS_ENABLED:
                ingress = tasks.ingress_chunk.s(index).set(**helpers.DEFAULT_QUEUE)

                chain.append(ingress)

            if process_func is not None:
                process = process_func.s(index).set(**helpers.DEFAULT_QUEUE)

                chain.append(process)

            if len(chain) > 0:
                process_chains.append(celery.chain(*chain))

                tasks_total += len(chain)

        context.job.step_inc(tasks_total)

        return process_chains

    def execute_process(self, context, process_func):
        start = tasks.job_started.s(context).set(**helpers.DEFAULT_QUEUE)

        units = tasks.base_units.s().set(**helpers.DEFAULT_QUEUE)

        merge = tasks.merge.s().set(**helpers.DEFAULT_QUEUE)

        preprocess_chains = self.generate_preprocess_chains(context)

        preprocess = start | units | celery.group(preprocess_chains) | merge

        context.job.step_inc(3)

        concat = tasks.concat.s().set(**helpers.DEFAULT_QUEUE)

        success = tasks.job_succeeded.s().set(**helpers.DEFAULT_QUEUE)

        cache = tasks.ingress_cache.s().set(**helpers.DEFAULT_QUEUE)

        cleanup = tasks.ingress_cleanup.s().set(**helpers.DEFAULT_QUEUE)

        finalize = concat | success | cache | cleanup

        context.job.step_inc(4)

        if context.is_compute:
            process_chains = self.generate_process_chains(context, process_func)
        else:
            process_chains = self.generate_process_chains(context)

        if len(process_chains) > 0:
            canvas = preprocess | celery.group(process_chains) | finalize
        else:
            canvas = preprocess | finalize

        canvas.delay()

    def execute(self, identifier, variable, domain, operation, process, job, user):
        logger.info('Identifier %r', identifier)

        logger.info('Variable %r', variable)

        logger.info('Domain %r', domain)

        logger.info('Operation %r', operation)

        if identifier == 'CDAT.workflow':
            context = WorkflowOperationContext.from_data_inputs(variable,
                                                                domain,
                                                                operation)

            context.job = job

            context.user = user

            context.process = process

            self.execute_workflow(identifier, context)
        else:
            process_func = base.get_process(identifier)

            if int(process_func.METADATA.get('inputs', 0)) == 0:
                context = OperationContext()

                context.job = job

                context.user = user

                context.process = process

                self.execute_simple(context, process_func)
            else:
                context = OperationContext.from_data_inputs(identifier, variable,
                                                            domain, operation)

                context.job = job

                context.user = user

                context.process = process

                self.execute_process(context, process_func)
