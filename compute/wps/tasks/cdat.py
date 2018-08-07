#! /usr/bin/env python

import datetime
import json
import os
import re
import uuid

import cdms2
import cwt
from cdms2 import MV2 as MV
from celery.task.control import inspect
from celery.utils.log import get_task_logger
from django.conf import settings
from django.utils import timezone

from wps import helpers
from wps import models
from wps import WPSError
from wps.tasks import base

logger = get_task_logger('wps.tasks.cdat')

OUTPUT = cwt.wps.process_output_description('output', 'output', 'application/json')

PATTERN_AXES_REQ = 'CDAT\.(min|max|average|sum)'

@base.cwt_shared_task()
def cleanup(self, attrs, file_paths, job_id):
    for path in file_paths:
        try:
            os.remove(path)
        except:
            logger.exception('Failed to remove %r', path)

    return attrs

@base.register_process('CDAT.health', abstract="""
Returns current server health
""", data_inputs=[], metadata={'inputs': 0})
@base.cwt_shared_task()
def health(self, user_id, job_id, **kwargs):
    job = self.load_job(job_id)

    user = self.load_user(user_id)

    i = inspect()

    active = i.active()

    jobs_running = sum(len(x) for x in active.values())

    scheduled = i.scheduled()

    reserved = i.reserved()

    jobs_scheduled = sum(len(x) for x in scheduled.values())

    jobs_reserved = sum(len(x) for x in reserved.values())

    users = models.User.objects.all()

    threshold = timezone.now() - settings.ACTIVE_USER_THRESHOLD

    def active(user):
        return user.last_login >= threshold

    active_users = [x for x in users if active(x)]

    data = {
        'jobs_running': jobs_running,
        'jobs_queued': jobs_scheduled+jobs_reserved,
        'active_users': len(active_users),
    }

    job.succeeded(json.dumps(data))

    return data

def read_data(infile, var_name, domain=None):
    if domain is None:
        data = infile(var_name)
    else:
        data = infile(var_name, **domain)

    return data

def write_data(data, var_name, base_units, grid, gridder, outfile):
    # Don't always need to adjust the time axis
    if base_units is not None:
        data.getTime().toRelativeTime(str(base_units))

    if grid is not None:
        data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

        logger.info('Regrid to shape %r', data.shape)

    outfile.write(data, id=var_name)

def base_retrieve(self, attrs, keys, operation, var_name, base_units, output_path, job_id):
    """ Retrieve file(s).

    This is the base for aggregate, subset and regrid.

    Expected format for "attrs" argument.

    {
        "key": {
            "path": "https://aims3.llnl.gov/path/filename.nc",
            "chunked_axis": "time",
            "chunks": {
                "time": [slice(0, 10), slice(10, 12)],
            },
            "mapped": {
                "time": slice(0, 10),
                "lat": slice(0, 100),
                "lon": slice(0, 200),
            },
        }
    }

    Args:
        attrs: A list of dict or dict from previous tasks.
        cached: A list of dict of cached portions.
        operation: A cwt.Process object.
        var_name: A str variable name.
        base_units: A str base_units to be used.
        job_id: An int of the current job id.

    Returns:
        The input attrs.
    """
    gridder = operation.get_parameter('gridder')

    job = self.load_job(job_id)

    if not isinstance(attrs, list):
        attrs = [attrs]

    attrs = dict(y for x in attrs for y in x.items())

    grid = None
    generated_grid = True

    with self.open(output_path, 'w') as outfile:
        # Expect the keys to be given in a sortable format
        for key in sorted(keys):
            current = attrs[key]

            url = current['path']

            with self.open(url) as infile:
                # If the file is cached we still read it in chunks to reduce
                # memory usage
                if 'cached' in current:
                    chunk_axis = current['chunk_axis']

                    chunk_list = current['chunk_list']

                    mapped = current['mapped']

                    for chunk in chunk_list:
                        # Update the map with the chunked axis
                        mapped.update({ chunk_axis: chunk })

                        data = read_data(infile, var_name, mapped)

                        if generated_grid:
                            generated_grid = False

                            grid = self.generate_grid(gridder, data)

                        write_data(data, var_name, base_units, grid, gridder, outfile)
                else:
                    data = read_data(infile, var_name)

                    if generated_grid:
                        generated_grid = False

                        grid = self.generate_grid(gridder, data)

                    write_data(data, var_name, base_units, grid, gridder, outfile)

    return attrs

def base_process(self, attrs, key, operation, var_name, base_units, axes, output_path, job_id):
    """ Process file.

    Expected format for attrs argument.

    {
        "key": {
            "path": "file:///path/filename.nc",
            "mapped": {
                "time": slice(0, 200, 1),
                ...
            }
        }
    }

    Args:
        attrs: A dict describing the file to be processed.
        key: A key used to identify the data in attrs.
        operation: A cwt.Process object.
        var_name: A str variable name.
        base_units: A str units used to base the time axis.
        axes: A list of str names of axes to operate over.
        output_path: A str containing the output path.
        job_id: An int of the current job id. 

    Returns:
        The input attrs dict.
    """
    job = self.load_job(job_id)

    inp = attrs[key]

    gridder = operation.get_parameter('gridder')

    if 'cached' in inp:
        mapped = inp['mapped']
    else:
        mapped = {}

    # Read input data
    with cdms2.open(inp['path']) as infile:
        data = infile(var_name, **mapped)

    # Generate grid if needed
    if gridder is not None:
        grid = self.generate_grid(gridder, data)

        data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

    # Grab the indexes of the axes
    axes_index = [data.getAxisIndex(str(x)) for x in axes]

    # Process axes
    for axis in axes_index:
        data = self.PROCESS(data, axis=axis)

    with cdms2.open(output_path, 'w') as outfile:
        outfile.write(data, id=var_name)

    return attrs

@base.cwt_shared_task()
def concat_process_output(self, attrs, input_paths, var_name, chunked_axis, output_path, job_id):
    """ Concatenates inputs over chunked_axis.

    Args:
        attrs: A dict or list of dicts from previous tasks.
        input_paths: A list of inputs to be concatenated.
        var_name: A str variable name.
        chunked_axis: A str name of the chunked axis.
        output_path: A str path to write the output to.
        job_id: An int referencing the associated job.

    Returns:
        The input attrs value.
    """
    job = self.load_job(job_id)

    data_list = []

    for input_path in sorted(input_paths):
        with cdms2.open(input_path) as infile:
            data_list.append(infile(var_name))

    axis_index = data_list[0].getAxisIndex(chunked_axis)

    data = MV.concatenate(data_list, axis=axis_index)

    with cdms2.open(output_path, 'w') as outfile:
        outfile.write(data, id=var_name)

    new_attrs = {}

    for item in attrs:
        new_attrs.update(item)

    return new_attrs

SNG_DATASET_SNG_INPUT = {
    'datasets': 1,
    'inputs': 1,
}

SNG_DATASET_MULTI_INPUT = {
    'datasets': 1,
    'inputs': '*',
}

@base.register_process('CDAT.regrid', abstract="""
Regrids a variable to designated grid. Required parameter named "gridder".
""", metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def regrid(self, attrs, keys, operation, var_name, base_units, output_path, job_id):
    return base_retrieve(self, attrs, keys, operation, var_name, base_units, output_path, job_id)

@base.register_process('CDAT.subset', abstract="""
Subset a variable by provided domain. Supports regridding.
""", metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def subset(self, attrs, keys, operation, var_name, base_units, output_path, job_id):
    return base_retrieve(self, attrs, keys, operation, var_name, base_units, output_path, job_id)

@base.register_process('CDAT.aggregate', abstract="""
Aggregate a variable over multiple files. Supports subsetting and regridding.
""", metadata=SNG_DATASET_MULTI_INPUT)
@base.cwt_shared_task()
def aggregate(self, attrs, keys, operation, var_name, base_units, output_path, job_id):
    return base_retrieve(self, attrs, keys, operation, var_name, base_units, output_path, job_id)

@base.register_process('CDAT.average', abstract=""" 
Computes the average over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""", process=MV.average, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def average(self, attrs, key, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, key, operation, var_name, base_units, axes, output_path, job_id)

@base.register_process('CDAT.sum', abstract=""" 
Computes the sum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""", process=MV.sum, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def summation(self, attrs, key, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, key, operation, var_name, base_units, axes, output_path, job_id)

@base.register_process('CDAT.max', abstract=""" 
Computes the maximum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""", process=MV.max, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def maximum(self, attrs, key, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, key, operation, var_name, base_units, axes, output_path, job_id)

@base.register_process('CDAT.min', abstract="""
Computes the minimum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""", process=MV.min, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def minimum(self, attrs, key, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, key, operation, var_name, base_units, axes, output_path, job_id)
