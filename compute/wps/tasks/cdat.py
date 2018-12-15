#! /usr/bin/env python

import datetime
import json
import os
import re
import uuid
from collections import deque
from datetime import datetime

import cdms2
import cwt
import cdutil
from cdms2 import MV2 as MV
from celery.task.control import inspect
from celery.utils.log import get_task_logger
from django.conf import settings
from django.utils import timezone

from wps import helpers
from wps import metrics
from wps import models
from wps import WPSError
from wps.tasks import base
from wps.context import OperationContext

logger = get_task_logger('wps.tasks.cdat')

def build_execute_graph(self, operation, job):
    self.update(job, 'Building execution graph')

    start = datetime.now()

    adjacency = dict((x, dict((y, True if x in operation[y].inputs else False)
                              for y in operation.keys())) for x in operation.keys())


    sources = [x for x in operation.keys() if not any(adjacency[y][x] for y
                                                        in operation.keys())]

    sorted = []

    while len(sources) > 0:
        item = sources.pop()

        sorted.append(operation[item])

        for x in adjacency[item].keys():
            if adjacency[item][x]:
                sources.append(x)

    elapsed = datetime.now() - start

    self.update(job, 'Finished building execution graph {}', elapsed)

    return deque(sorted)

def prepare_operation(variable, domain, op):
    op.inputs = [variable[x] for x in op.inputs]

    op.domain = domain.get(op.domain, None)

    if 'domain' in op.parameters:
        del op.parameters['domain']

    return op

def wait_for_inputs(self, op, variable, job, executing, output, **kwargs):
    for x in op.inputs:
        if x in executing:
            wait_op = executing[x]

            result = wait_op.wait()

            if not result:
                raise WPSError('Operation "{}" failed due to missing input from'
                               '"{}"', op.identifier, wait_op.identifier)

            self.update(job, '{!r} finished with output {!r}', wait_op.identifier,
                        wait_op.output)

            del executing[x]

            name = '{}-{}'.format(wait_op.identifier, wait_op.name)

            new_variable = cwt.Variable(wait_op.output.uri,
                                        wait_op.output.var_name, name=name)

            output.append(new_variable)

            variable[wait_op.name] = new_variable

@base.register_process('CDAT.workflow', metadata={}, hidden=True)
@base.cwt_shared_task()
def workflow(self, variable, domain, operation, user_id, job_id, **kwargs):
    user = self.load_user(user_id)

    job = self.load_job(job_id)

    sorted = build_execute_graph(self, operation, job)

    client = cwt.WPSClient(settings.WPS_ENDPOINT, api_key=user.auth.api_key,
                           verify=False)

    state = {
        'executing': {},
        'output': [],
    }

    while len(sorted) > 0:
        op = sorted.popleft()

        if not all(x in variable for x in op.inputs):
            wait_for_inputs(self, op, variable, job, **state)

        op = prepare_operation(variable, domain, op)

        client.execute(op, **op.parameters)

        self.update(job, 'Executing "{}"', op.identifier)

        state['executing'][op.name] = op

    for x in state['executing'].values():
        result = x.wait()

        if not result:
            raise WPSError('Operation "{}" failed due to missing input from'
                           '"{}"', op.identifier, x.identifier)

        self.update(job, '{!r} finished with output {!r}', x.identifier,
                    x.output)

        name = '{}-{}'.format(x.identifier, x.name)

        new_variable = cwt.Variable(x.output.uri,
                                    x.output.var_name, name=name)

        state['output'].append(new_variable)

    self.update(job, 'Finished executing workflow')

    attrs = {
        'output': state['output'],
    }

    return attrs

SNG_DATASET_SNG_INPUT = {
    'datasets': 1,
    'inputs': 1,
}

SNG_DATASET_MULTI_INPUT = {
    'datasets': 1,
    'inputs': '*',
}

REGRID_ABSTRACT = """
Regrids a variable to designated grid. Required parameter named "gridder".
"""

SUBSET_ABSTRACT = """
Subset a variable by provided domain. Supports regridding.
"""

AGGREGATE_ABSTRACT = """
Aggregate a variable over multiple files. Supports subsetting and regridding.
"""

AVERAGE_ABSTRACT = """
Computes the average over axes. 

Required parameters:
 axes: A list of axes to operate on. Should be separated by "|".

Optional parameters:
 weightoptions: A string whos value is "generate",
   "equal", "weighted", "unweighted". See documentation
   at https://cdat.llnl.gov/documentation/utilities/utilities-1.html
"""

SUM_ABSTRACT = """
Computes the sum over an axis. Requires singular parameter named "chunked_axis, axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
"""

MAX_ABSTRACT = """
Computes the maximum over an axis. Requires singular parameter named "chunked_axis, axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
"""

MIN_ABSTRACT = """
Computes the minimum over an axis. Requires singular parameter named "chunked_axis, axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
"""

@base.cwt_shared_task()
def concat(self, contexts):
    context = OperationContext.merge_ingress(contexts)

    grid = None
    
    gridder = context.operation.get_parameter('gridder')

    context.output_path = context.gen_public_path()

    with context.new_output(context.output_path) as outfile:
        for input in context.sorted_inputs():
            logger.info('%r', input.variable.uri)

            for _, chunk in input.chunks(context):
                if grid is None and gridder is not None:
                    grid = self.generate_grid(gridder)

                    grid = self.subset_grid(grid, input.mapped)

                if grid is not None:
                    shape = chunk.shape

                    chunk = chunk.regrid(grid, regridTool=gridder.tool,
                                         regridMethod=gridder.method)

                    logger.info('Regrid %r -> %r', shape, chunk.shape)

                if context.units is not None:
                    chunk.getTime().toRelativeTime(str(context.units))

                logger.info('Chunk shape %r', chunk.shape)

                outfile.write(chunk, id=input.variable.var_name)

    return context

@base.register_process('CDAT.regrid', abstract=REGRID_ABSTRACT, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def regrid(self, context):
    """ Regrids a chunk of data.
    """
    return context

@base.register_process('CDAT.subset', abstract=SUBSET_ABSTRACT, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def subset(self, context):
    """ Subsetting data.
    """
    return context

@base.register_process('CDAT.aggregate', abstract=AGGREGATE_ABSTRACT, metadata=SNG_DATASET_MULTI_INPUT)
@base.cwt_shared_task()
def aggregate(self, context):
    """ Aggregating data.
    """
    return context

@base.register_process('CDAT.average', abstract=AVERAGE_ABSTRACT, process=cdutil.averager, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def average(self, context):
    return context

@base.register_process('CDAT.sum', abstract=SUM_ABSTRACT, process=MV.sum, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def summation(self, context):
    return context

@base.register_process('CDAT.max', abstract=MAX_ABSTRACT, process=MV.max, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def maximum(self, context, index):
    base = 0
    
    axes = context.operation.get_parameter('axes', True)

    for input in context.sorted_inputs():
        indices = self.generate_indices(index, len(input.chunk))

        for index, chunk in input.chunks(context, indices):
            local_index = base + index

            local_filename = 'data_{}_{:08}_{}.nc'.format(str(context.job.id),
                                                          local_index,
                                                          '_'.join(axes.values))

            local_path = context.gen_ingress_path(local_filename)

            for axis in axes.values:
                axis_index = chunk.getAxisIndex(axis)

                if axis_index == -1:
                    raise WPSError('Unknown axis {!s}', axis)

                chunk = MV.max(chunk, axis=axis_index)

            with context.new_output(local_path) as outfile:
                outfile.write(chunk, id=input.variable.var_name)

            input.process.append(local_path)

        base = len(input.chunk)

    return context

@base.register_process('CDAT.min', abstract=MIN_ABSTRACT, process=MV.min, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def minimum(self, context):
    return context
