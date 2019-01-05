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

@base.register_process('CDAT.workflow', metadata={}, hidden=True)
@base.cwt_shared_task()
def workflow(self, context):
    client = cwt.WPSClient(settings.WPS_ENDPOINT, api_key=context.user.auth.api_key,
                           verify=False)

    queue = context.build_execute_graph()

    while len(queue) > 0:
        next = queue.popleft()

        context.wait_for_inputs(next)

        context.prepare(next)

        # Here we can make the choice on which client to execute
        client.execute(next)

        context.add_executing(next)

    context.wait_remaining()

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
    axes = context.operation.get_parameter('axes', True)

    grid = None
    
    gridder = context.operation.get_parameter('gridder')

    for input_index, input in enumerate(context.sorted_inputs()):
        for _, chunk_index, chunk in input.chunks(index, context):
            if grid is None and gridder is not None:
                metrics.WPS_REGRID.labels(gridder.tool, gridder.method,
                                          gridder.grid).inc()

                grid = self.generate_grid(gridder)

                grid = self.subset_grid(grid, input.mapped)

            process_filename = '{}_{:08}_{:08}_{}.nc'.format(
                str(context.job.id), input_index, chunk_index, '_'.join(axes.values))

            process_path = context.gen_ingress_path(process_filename)

            if grid is not None:
                shape = chunk.shape

                chunk = chunk.regrid(grid, regridTool=gridder.tool,
                                     regridMethod=gridder.method)

                logger.info('Regrid %r -> %r', shape, chunk.shape)

            if process is not None:
                with metrics.WPS_PROCESS_TIME.labels(context.operation.identifier).time():
                    chunk = process(chunk, axes.values)

            with context.new_output(process_path) as outfile:
                outfile.write(chunk, id=input.variable.var_name)

            input.process.append(process_path)

    return context

def regrid_data(self, context, index):
    grid = None
    
    gridder = context.operation.get_parameter('gridder')

    for input in context.sorted_inputs():
        if gridder is not None:
            for source_path, _, chunk in input.chunks(index, context):
                if grid is None and gridder is not None:
                    metrics.WPS_REGRID.labels(gridder.tool, gridder.method,
                                              gridder.grid).inc()
                    
                    grid = self.generate_grid(gridder)

                    grid = self.subset_grid(grid, input.mapped)

                process_filename = parse_filename(source_path)

                process_filename = '{}_regrid.nc'.format(process_filename)

                process_path = context.gen_ingress_path(process_filename)

                if grid is not None:
                    shape = chunk.shape

                    chunk = chunk.regrid(grid, regridTool=gridder.tool,
                                         regridMethod=gridder.method)

                    logger.info('Regrid %r -> %r', shape, chunk.shape)

                if context.units is not None and chunk.getTime() is not None:
                    chunk.getTime().toRelativeTime(str(context.units))

                with context.new_output(process_path) as outfile:
                    outfile.write(chunk, id=input.variable.var_name)

                input.process.append(process_path)

    return context

def parse_filename(path):
    base = os.path.basename(path)

    filename, _ = os.path.splitext(base)

    return filename

@base.cwt_shared_task()
def concat(self, contexts):
    context = OperationContext.merge_ingress(contexts)

    context.output_path = context.gen_public_path()

    with context.new_output(context.output_path) as outfile:
        for input in context.sorted_inputs():
            data = []
            chunk_axis = None
            chunk_axis_index = None

            for file_path, _, chunk in input.chunks(context=context):
                logger.info('Chunk shape %r %r', file_path, chunk.shape)

                if chunk_axis is None:
                    chunk_axis_index = chunk.getAxisIndex(input.chunk_axis)

                    chunk_axis = chunk.getAxis(chunk_axis_index)

                if chunk_axis.isTime():
                    logger.info('Writing temporal chunk')

                    outfile.write(chunk, id=input.variable.var_name)
                else:
                    logger.info('Gathering spatial chunk')

                    data.append(chunk)

            if chunk_axis is not None and not chunk_axis.isTime():
                data = MV2.concatenate(data, axis=chunk_axis_index)

                outfile.write(data, id=input.variable.var_name)

    return context

@base.register_process('CDAT.regrid', abstract=REGRID_ABSTRACT, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def regrid(self, context, index):
    """ Regrids a chunk of data.
    """
    return regrid_data(self, context, index)

@base.register_process('CDAT.subset', abstract=SUBSET_ABSTRACT,
                       metadata=SNG_DATASET_MULTI_INPUT)
@base.cwt_shared_task()
def subset(self, context, index):
    """ Subsetting data.
    """
    return regrid_data(self, context, index)

@base.register_process('CDAT.aggregate', abstract=AGGREGATE_ABSTRACT, metadata=SNG_DATASET_MULTI_INPUT)
@base.cwt_shared_task()
def aggregate(self, context, index):
    """ Aggregating data.
    """
    return regrid_data(self, context, index)

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
