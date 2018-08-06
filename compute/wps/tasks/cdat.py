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

@base.register_process('CDAT.health', abstract="""
Returns current server health
""", data_inputs=[], metadata={'inputs': 0})
@base.cwt_shared_task()
def health(self, user_id, job_id, process_id, **kwargs):
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

def read_data(infile, var_name, domain):
    if domain is None:
        # Read whole file is domain is None, mostly used for ingress
        # files
        data = infile(var_name)
    else:
        # Attempt to grab the time dimension from the domain
        try:
            time = domain.pop('time')
        except KeyError:
            # Grab data without time dimension
            data = infile(var_name, **domain)
        else:
            data = infile(var_name, time=time, **domain)

    return data

def write_data(data, var_name, base_units, grid, gridder, outfile):
    # Don't always need to adjust the time axis
    if base_units is not None:
        data.getTime().toRelativeTime(str(base_units))

    if grid is not None:
        data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

        logger.info('Regrid to shape %r', data.shape)

    outfile.write(data, id=var_name)

def base_retrieve(self, attrs, keys, operation, var_name, base_units, job_id):
    """ Reconstructs file described by attrs and cached.

    Expected format for attrs argument.

    {
        "key": {
            "path": "https://aims3.llnl.gov/path/filename.nc",
            "ingress": {
                "path": "file:///path/filename.nc",
            }
            --- or ---
            "cached": {
                "path": "file:///path/filename.nc",
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
    }

    Args:
        attrs: A list of dict or dict from previous tasks.
        cached: A list of dict of cached portions.
        operation: A cwt.Process object.
        var_name: A str variable name.
        base_units: A str base_units to be used.
        job_id: An int of the current job id.

    Returns:
        A list of dicts, this should be the combination of attrs and cached 
        arguments.
    """
    gridder = operation.get_parameter('gridder')

    job = self.load_job(job_id)

    if not isinstance(attrs, list):
        attrs = [attrs]

    attrs = dict(y for x in attrs for y in x.items())

    output_name = '{}.nc'.format(uuid.uuid4())

    output_path = os.path.join(settings.WPS_LOCAL_OUTPUT_PATH, output_name)

    grid = None
    generated_grid = True

    with cdms2.open(output_path, 'w') as outfile:
        for key in sorted(keys):
            current = attrs[key]

            if 'cached' in current:
                url = current['cached']['path']
            else:
                url = current['ingress']['path']

            with cdms2.open(url) as infile:
                if 'cached' in current:
                    chunk_axis = current['cached']['chunk_axis']

                    chunk_list = current['cached']['chunk_list']

                    mapped = current['cached']['mapped']

                    for chunk in chunk_list:
                        mapped.update({ chunk_axis: chunk })

                        data = infile(var_name, **mapped)

                        if generated_grid:
                            generated_grid = False

                            grid = self.generate_grid(gridder, data)

                        write_data(data, var_name, base_units, grid, gridder, outfile)
                else:
                    data = infile(var_name)

                    if generated_grid:
                        generated_grid = False

                        grid = self.generate_grid(gridder, data)

                    write_data(data, var_name, base_units, grid, gridder, outfile)

    output_dap = settings.WPS_DAP_URL.format(filename=output_name)

    # Update dict with success
    attrs['output'] = cwt.Variable(output_dap, var_name)

    return attrs

def base_process(self, attrs, operation, var_name, base_units, axes, output_path, job_id):
    """ Process the file passed in attrs.

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
        operation: A cwt.Process object.
        var_name: A str variable name.
        base_units: A str containing the base units.
        axes: A list of str axis names to operate over.
        output_path: A str containing the output path.
        mv_func: An instance of the MV2 function.
        job_id: An int of the current job id. 

    Returns:
        A dict with the following format:

        {
            "key": {
                "process": {
                    "path": "file:///path/filename.nc", 
                }
            }
        }

    """
    job = self.load_job(job_id)

    inp = attrs.values()[0]

    gridder = operation.get_parameter('gridder')

    if 'ingress' in inp:
        ingress = inp['ingress']

        with cdms2.open(ingress['path']) as infile:
            data = infile(var_name)

        if gridder is not None:
            grid = self.generate_grid(gridder, data)

            data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

        if base_units is not None:
            data.getTime().toRelativeTime(str(base_units))

        axes_index = [data.getAxisIndex(str(x)) for x in axes]

        for axis in axes_index:
            data = self.PROCESS(data, axis=axis)

        with cdms2.open(output_path, 'w') as outfile:
            outfile.write(data, id=var_name)
    elif 'cached' in attrs:
        cached = attrs['cached']

        mapped = cached['mapped']

        with cdms2.open(cached['path']) as infile:
            data = infile(var_name, **mapped)

        if gridder is not None:
            grid = self.generate_grid(gridder, data)

            data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

        if base_units is not None:
            data.getTime().toRelativeTime(str(base_units))

        axes_index = [data.getAxisIndex(str(x)) for x in axes]

        for axis in axes_index:
            data = self.PROCESS(data, axis=axis)

        with cdms2.open(output_path, 'w') as outfile:
            outfile.write(data, id=var_name)
    else:
        raise WPSError('Something went wrong input was neither ingressed or cached')

    attrs['processed'] = output_path

    return attrs

@base.cwt_shared_task()
def concat_process_output(self, attrs, input_paths, var_name, chunked_axis, output_path, job_id):
    """ Concatenates the process outputs.

    Args:
        attrs: A dict or list of dicts from previous tasks.
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
def regrid(self, attrs, keys, operation, var_name, base_units, job_id):
    return base_retrieve(self, attrs, keys, operation, var_name, base_units, job_id)

@base.register_process('CDAT.subset', abstract="""
Subset a variable by provided domain. Supports regridding.
""", metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def subset(self, attrs, keys, operation, var_name, base_units, job_id):
    return base_retrieve(self, attrs, keys, operation, var_name, base_units, job_id)

@base.register_process('CDAT.aggregate', abstract="""
Aggregate a variable over multiple files. Supports subsetting and regridding.
""", metadata=SNG_DATASET_MULTI_INPUT)
@base.cwt_shared_task()
def aggregate(self, attrs, keys, operation, var_name, base_units, job_id):
    return base_retrieve(self, attrs, keys, operation, var_name, base_units, job_id)

@base.register_process('CDAT.average', abstract=""" 
Computes the average over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""", process=MV.average, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def average(self, attrs, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, operation, var_name, base_units, axes, output_path, job_id)

@base.register_process('CDAT.sum', abstract=""" 
Computes the sum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""", process=MV.sum, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def summation(self, attrs, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, operation, var_name, base_units, axes, output_path, job_id)

@base.register_process('CDAT.max', abstract=""" 
Computes the maximum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""", process=MV.max, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def maximum(self, attrs, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, operation, var_name, base_units, axes, output_path, job_id)

@base.register_process('CDAT.min', abstract="""
Computes the minimum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""", process=MV.min, metadata=SNG_DATASET_SNG_INPUT)
@base.cwt_shared_task()
def minimum(self, attrs, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, operation, var_name, base_units, axes, output_path, job_id)
