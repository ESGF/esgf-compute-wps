#! /usr/bin/env python

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
from wps.tasks import process
from wps.tasks import file_manager

logger = get_task_logger('wps.tasks.cdat')

OUTPUT = cwt.wps.process_output_description('output', 'output', 'application/json')

PATTERN_AXES_REQ = 'CDAT\.(min|max|average|sum)'

@base.cwt_shared_task()
def validate(self, attrs, job_id=None):
    root_op = attrs['root']

    operation = attrs['operation'][root_op]

    if operation.identifier == 'CDAT.regrid':
        # Might need to validate the gridder configuration
        if 'gridder' not in operation.parameters:
            raise WPSError('Missing required parameter "gridder"')
    elif re.match(PATTERN_AXES_REQ, operation.identifier) is not None:
        logger.info('Checking for axes parameter')

        axes = operation.get_parameter('axes')

        if axes is None:
            raise WPSError('Missing required parameter "axes"')

        for uri, mapping in attrs['mapped'].iteritems():
            for axis in axes.values:
                if mapping is not None and axis not in mapping:
                    raise WPSError('Missing "{axis}" from parameter "axes"', axis=axis)

    return attrs

@base.register_process('CDAT.health', abstract="""
Returns current server health
""", data_inputs=[])
@base.cwt_shared_task()
def health(self, user_id, job_id, process_id, **kwargs):
    self.PUBLISH = base.ALL

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id, process_id)

    proc.job.started()

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
        'data': {
            'jobs_running': jobs_running,
            'jobs_queued': jobs_scheduled+jobs_reserved,
            'active_users': len(active_users),
        }
    }

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

def write_data(data, var_name, base_units, outfile):
    data.getTime().toRelativeTime(str(base_units))

    if grid is not None:
        data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

        logger.info('Regrid to shape %r', data.shape)

    outfile.write(data, id=var_name)

def base_retrieve(self, attrs, cached, operation, var_name, base_units, job_id):
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

    logger.info('Gridder %r', gridder)

    job = self.load_job(job_id)

    if not isinstance(attrs, list):
        attrs = [attrs,]

    attrs.extend(cached)

    output_name = '{}.nc'.format(uuid.uuid4())

    output_path = os.path.join(settings.WPS_LOCAL_OUTPUT_PATH, output_name)

    try_grid = False
    grid = None

    logger.info('Output path %r', output_path)

    with cdms2.open(output_path, 'w') as outfile:
        attrs = sorted(attrs, key=lambda x: x['key'])

        for item in attrs:
            if 'cached' in item:
                uri = items['cached']['path']
            else:
                uri = items['ingress']['path']

            logger.info('Opening %r', uri)

            with cdms2.open(uri) as infile:
                if 'cached' in item:
                    mapped = item['cached']['mapped']

                    chunked_axis = item['cached']['chunked_axis']

                    chunks = item['cached']['chunks']

                    for chunk in chunks:
                        mapped.update({ chunked_axis: chunk })

                        mapped_copy = mapped.copy()

                        time = mapped_copy.pop('time')

                        data = infile(var_name, time=time, **mapped_copy)

                        if not try_grid:
                            try_grid = True

                            grid = self.generate_grid(gridder, data)

                        if grid is not None:
                            data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

                        write_data(data, var_name, base_units, outfile)
                else:
                    data = infile(var_name)

                    if not try_grid:
                        try_grid = True

                        grid = self.generate_grid(gridder, data)

                    if grid is not None:
                        data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

                    write_data(data, var_name, base_units, outfile)

        logger.info('Final shape %r', outfile[var_name].shape)

    output_dap = settings.WPS_DAP_URL.format(filename=output_name)

    var = cwt.Variable(output_dap, var_name)

    logger.info('Marking job complete with output %r', var)

    job.succeeded(json.dumps(var.parameterize()))

    return attrs

def base_process(self, attrs, operation, var_name, base_units, axes, output_path, mv_func, job_id):
    """ Process the file passed in attrs.

    Expected format for attrs argument.

    {
        "key": {
            "ingress": {
                "path": "file:///path/filename.nc",
            }
            --- or ---
            "cached": {
                "path": "file:///path/filename.nc",
                "domain": {
                    "time": slice(0, 10),
                    "lat": slice(0, 100),
                    "lon": slice(0, 200),
                }
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
    if not isinstance(attrs, dict):
        raise WPSError('Input from previous task should be type dict got type "{name}"', name=type(attrs))

    key = attrs.keys()[0]

    value = attrs[key]

    logger.info('Processing key %r', key)

    if 'ingress' in value:
        logger.info('Source was ingressed')

        uri = value['ingress']['path']

        domain = None
    elif 'cached' in value:
        logger.info('Source was cached')

        uri = value['cached']['path']

        domain = value['cached']['domain']
    else:
        raise WPSError('Missing required "ingress" or "cached" key')

    logger.info('Reading input data from %r', uri)

    # Read input data
    try:
        with cdms2.open(uri) as infile:
            data = read_data(infile, var_name, domain)
    except cdms2.CDMSError:
        logger.exception('Failed to read input file %r', uri)

        raise WPSError('Error opening "{uri}"', uri=uri)

    logger.info('Input data shape %r', data.shape)

    axis_indexes = [data.getAxisIndex(axis) for axis in axes]

    if any([x < 0 for x in axis_indexes]):
        raise WPSError('Missing axis "{name}"', axis)

    logger.info('All axes %r are present in file', axes)

    gridder = operation.get_parameter('gridder')

    grid = self.generate_grid(gridder, data)

    # Regrid before processing data
    if grid is not None:
        logger.info('Target grid shape %r', grid.shape)

        data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

        logger.info('Data shape %r', data.shape)

    # Remap the time axis if passed the base units
    if base_units is not None:
        data.getTime().toRelativeTime(str(base_units))

        logger.info('Remapped time axis with base units %r', base_units)

    logger.info('Begin processing data')

    for axis in axis_indexes:
        data = mv_func(data, axis=axis)

        logger.info('Processed over %r output shape %r', axis, data.shape)

    logger.info('End processing data, writing output %r', output_path)

    # Write output data
    try:
        with cdms2.open(output_path, 'w') as outfile:
            outfile.write(data, id=var_name)
    except cdms2.CDMSError:
        logger.exception('Failed to write output %r', output_path)

        raise WPSError('Error writing "{uri}"', uri=output_path)

    attrs[key].update({
        'processed': {
            'path': output_path,
        }
    })

    logger.info('Returning %r', attrs)

    return attrs

@base.cwt_shared_task()
def concat_process_output(self, attrs, var_name, job_id):
    """ Concatenates the process outputs.

    Args:
        attrs: A dict or list of dicts from previous tasks.
        job_id: An int referencing the associated job.

    Returns:
        The input attrs value.
    """
    job = self.load_job(job_id)

    if not isinstance(attrs, list):
        attrs = [attrs]

    attrs = sorted(attrs, key=lambda x: x.values()[0]['processed']['path'])

    output_name = '{}.nc'.format(uuid.uuid4())

    output_path = os.path.join(settings.WPS_LOCAL_OUTPUT_PATH, output_name)

    try:
        with cdms2.open(output_path, 'w') as outfile:
            for item in attrs:
                key = item.keys()[0]

                input_path = item[key]['processed']['path']

                try:
                    with cdms2.open(input_path) as infile:
                        data = infile(var_name)

                        outfile.write(data, id=var_name)
                except cdms2.CDMSError:
                    raise WPSError('Failed to open input file "{path}"', path=input_path)
    except cdms2.CDMSError:
        raise WPSError('Failed to open output file "{path}"', path=output_path)

    output_dap = settings.WPS_DAP_URL.format(filename=output_name)

    var = cwt.Variable(output_dap, var_name)

    logger.info('Marking job complete with output %r', var)

    job.succeeded(json.dumps(var.parameterize()))

    return attrs

@base.register_process('CDAT.regrid', abstract="""
Regrids a variable to designated grid. Required parameter named "gridder".
""")
@base.cwt_shared_task()
def regrid(self, attrs, cached, operation, var_name, base_units, job_id=None):
    return base_retrieve(self, attrs, cached, operation, var_name, base_units, job_id)

@base.register_process('CDAT.subset', abstract="""
Subset a variable by provided domain. Supports regridding.
""")
@base.cwt_shared_task()
def subset(self, attrs, cached, operation, var_name, base_units, job_id=None):
    return base_retrieve(self, attrs, cached, operation, var_name, base_units, job_id)

@base.register_process('CDAT.aggregate', abstract="""
Aggregate a variable over multiple files. Supports subsetting and regridding.
""")
@base.cwt_shared_task()
def aggregate(self, attrs, cached, operation, var_name, base_units, job_id=None):
    return base_retrieve(self, attrs, cached, operation, var_name, base_units, job_id)

@base.register_process('CDAT.average', abstract=""" 
Computes the average over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def average(self, attrs, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, operation, var_name, base_units, axes, output_path, MV.average, job_id)

@base.register_process('CDAT.sum', abstract=""" 
Computes the sum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def sum(self, attrs, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, operation, var_name, base_units, axes, output_path, MV.sum, job_id)

@base.register_process('CDAT.max', abstract=""" 
Computes the maximum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def maximum(self, attrs, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, operation, var_name, base_units, axes, output_path, MV.max, job_id)

@base.register_process('CDAT.min', abstract="""
Computes the minimum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
                       """)
@base.cwt_shared_task()
def minimum(self, attrs, operation, var_name, base_units, axes, output_path, job_id):
    return base_process(self, attrs, operation, var_name, base_units, axes, output_path, MV.min, job_id)

@base.cwt_shared_task()
def cache_variable(self, parent_variables, variables, domains, operation, user_id, job_id):
    self.PUBLISH = base.RETRY | base.FAILURE

    _, _, o = self.load(parent_variables, variables, domains, operation)

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id)

    proc.job.started()

    output_name = '{}.nc'.format(str(uuid.uuid4()))

    output_path = os.path.join(settings.WPS_LOCAL_OUTPUT_PATH, output_name)

    try:
        with cdms2.open(output_path, 'w') as output_file:
            output_var_name = proc.retrieve(o, None, output_file)
    except cdms2.CDMSError as e:
        raise base.AccessError(output_path, e.message)
    except WPSError:
        raise

    if settings.WPS_DAP:
        output_url = settings.WPS_DAP_URL.format(filename=output_name)
    else:
        output_url = settings.WPS_OUTPUT_URL.format(filename=output_name)

    output_variable = cwt.Variable(output_url, output_var_name).parameterize()

    return {o.name: output_variable}
