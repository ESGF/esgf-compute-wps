#! /usr/bin/env python

import datetime
import json
import os
import re
import uuid
from collections import deque

import cdms2
import cwt
import cdutil
from cdms2 import MV2
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

@base.register_process('CDAT.workflow', metadata={})
@base.cwt_shared_task()
def workflow(self, context):
    """ Executes a workflow.

    Process a forest of operations. The graph can have multiple inputs and
    outputs.

    Args:
        context (WorkflowOperationContext): Current context.

    Returns:
        Updated context.
    """
    client = cwt.WPSClient(settings.WPS_ENDPOINT, api_key=context.user.auth.api_key,
                           verify=False)

    queue = context.build_execute_graph()

    while len(queue) > 0:
        next = queue.popleft()

        completed = context.wait_for_inputs(next)

        if len(completed) > 0:
            completed_ids = ', '.join('-'.join([x.identifier, x.name]) for x in completed)

            self.status('Processes {!s} have completed', completed_ids)

        context.prepare(next)

        # TODO distributed workflows
        # Here we can make the choice on which client to execute
        client.execute(next)

        context.add_executing(next)

        self.status('Executing process {!s}-{!s}', next.identifier, next.name)

    completed = context.wait_remaining()

    if len(completed) > 0:
        completed_ids = ', '.join('-'.join([x.identifier, x.name]) for x in completed)

        self.status('Processes {!s} have completed', completed_ids)

    return context

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

def process_data(self, context, index, process):
    """ Process a chunks of data.

    Function passed as process should accept two arguments, the first being a 
    cdms2.TransientVariable and the second a list of axis names.

    Args:
        context (OperationContext): Current context.
        index (int): Worker index used to determine the portion of work to complete.
        process (function): A function to process the data, see above for arguments.

    Returns:
        Updated context.
    """
    axes = context.operation.get_parameter('axes', True)

    nbytes = 0

    for input_index, input in enumerate(context.sorted_inputs()):
        for _, chunk_index, chunk in input.chunks(index, context):
            nbytes += chunk.nbytes

            process_filename = '{}_{:08}_{:08}_{}.nc'.format(
                str(context.job.id), input_index, chunk_index, '_'.join(axes.values))

            process_path = context.gen_ingress_path(process_filename)

            if process is not None:
                # Track processing time
                with metrics.WPS_PROCESS_TIME.labels(context.operation.identifier).time():
                    chunk = process(chunk, axes.values)

            with context.new_output(process_path) as outfile:
                outfile.write(chunk, id=input.variable.var_name)

            input.process.append(process_path)

    self.status('Processed {!r} bytes', nbytes)

    return context

def parse_filename(path):
    """ Parses filename from path.

    /data/hello.nc -> hello

    Args:
        path (string): A path to parse.

    Retunrs:
        str: The base filename
    """
    base = os.path.basename(path)

    filename, _ = os.path.splitext(base)

    return filename

def regrid_chunk(context, chunk, selector):
    """ Regrids a chunk of data.

    Args:
        context (OperationContext): Current context.
        chunk (cdms2.TransientVariable): Chunk of data to be regridded.
        selector (dict): A dict describing the portion of data we want.

    Returns:
        cdms2.TransientVariable: Regridded varibale.
    """
    grid, tool, method = context.regrid_context(selector)

    shape = chunk.shape

    chunk = chunk.regrid(grid, regridTool=tool, regridMethod=method)

    logger.info('Regrid %r -> %r', shape, chunk.shape)

    return chunk

@base.cwt_shared_task()
def concat(self, contexts):
    """ Concatenate data chunks.

    Args:
        context (OperationContext): Current context.

    Returns:
        Updated context.
    """
    context = OperationContext.merge_ingress(contexts)

    context.output_path = context.gen_public_path()

    nbytes = 0
    start = datetime.datetime.now()

    with context.new_output(context.output_path) as outfile:
        for index, input in enumerate(context.sorted_inputs()):
            data = []
            chunk_axis = None
            chunk_axis_index = None

            # Skip file if not mapped
            if input.mapped is None:
                continue

            for file_path, _, chunk in input.chunks(index=index, context=context):
                logger.info('Chunk shape %r %r', file_path, chunk.shape)

                if chunk_axis is None:
                    chunk_axis_index = chunk.getAxisIndex(input.chunk_axis)

                    chunk_axis = chunk.getAxis(chunk_axis_index)

                # We can write chunks along the temporal axis immediately
                # otherwise we need to collect them to concatenate over
                # an axis
                if chunk_axis.isTime():
                    logger.info('Writing temporal chunk %r', chunk.shape)

                    if context.units is not None:
                        chunk.getTime().toRelativeTime(str(context.units))

                    if context.is_regrid:
                        chunk = regrid_chunk(context, chunk, input.mapped)

                    outfile.write(chunk, id=str(input.variable.var_name))
                else:
                    logger.info('Gathering spatial chunk')

                    data.append(chunk)

                nbytes += chunk.nbytes

            # Concatenate chunks along an axis
            if chunk_axis is not None and not chunk_axis.isTime():
                data = MV2.concatenate(data, axis=chunk_axis_index)

                if context.is_regrid:
                    chunk = regrid_chunk(context, chunk, input.mapped)

                outfile.write(data, id=str(input.variable.var_name))

                nbytes += chunk.nbytes

    elapsed = datetime.datetime.now() - start

    self.status('Processed {!r} bytes in {!r} seconds', nbytes, elapsed.total_seconds())

    return context

@base.register_process('CDAT.regrid', abstract=REGRID_ABSTRACT, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def regrid(self, context, index):
    """ Regrids a chunk of data.
    """
    return context

@base.register_process('CDAT.subset', abstract=SUBSET_ABSTRACT, metadata=SNG_DATASET_MULTI_INPUT)
@base.cwt_shared_task()
def subset(self, context, index):
    """ Subsetting data.
    """
    return context

@base.register_process('CDAT.aggregate', abstract=AGGREGATE_ABSTRACT, metadata=SNG_DATASET_MULTI_INPUT)
@base.cwt_shared_task()
def aggregate(self, context, index):
    """ Aggregating data.
    """
    return context

@base.register_process('CDAT.average', abstract=AVERAGE_ABSTRACT, process=cdutil.averager, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def average(self, context, index):
    def average_func(data, axes):
        axis_indices = []

        for axis in axes:
            axis_index = data.getAxisIndex(axis)

            if axis_index == -1:
                raise WPSError('Unknown axis {!s}', axis)

            axis_indices.append(str(axis_index))

        axis_sig = ''.join(axis_indices)

        data = cdutil.averager(data, axis=axis_sig)

        return data

    return process_data(self, context, index, average_func)

@base.register_process('CDAT.sum', abstract=SUM_ABSTRACT, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def sum(self, context, index):
    def sum_func(data, axes):
        for axis in axes:
            axis_index = data.getAxisIndex(axis)

            if axis_index == -1:
                raise WPSError('Unknown axis {!s}', axis)

            data = MV2.sum(data, axis=axis_index)

        return data

    return process_data(self, context, index, sum_func)

@base.register_process('CDAT.max', abstract=MAX_ABSTRACT, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def max(self, context, index):
    def max_func(data, axes):
        for axis in axes:
            axis_index = data.getAxisIndex(axis)

            if axis_index == -1:
                raise WPSError('Unknown axis {!s}', axis)

            data = MV2.max(data, axis=axis_index)

        return data

    return process_data(self, context, index, max_func)

@base.register_process('CDAT.min', abstract=MIN_ABSTRACT, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def min(self, context, index):
    def min_func(data, axes):
        for axis in axes:
            axis_index = data.getAxisIndex(axis)

            if axis_index == -1:
                raise WPSError('Unknown axis {!s}', axis)

            data = MV2.min(data, axis=axis_index)

        return data

    return process_data(self, context, index, min_func)
