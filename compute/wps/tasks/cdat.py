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

def write_data(data, var_name, gridder, grid, base_units, outfile):
    logger.info('Read data shape %r', data.shape)

    if gridder is not None and grid is None:
        grid = self.generate_grid(gridder, data)

        logger.info('Generated grid shape %r', grid.shape)

    data.getTime().toRelativeTime(str(base_units))

    if grid is not None:
        data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

        logger.info('Regrid to shape %r', data.shape)

    outfile.write(data, id=var_name)

def base_retrieve(self, attrs, cached, operation, var_name, base_units, job_id):
    gridder = operation.get_parameter('gridder')

    logger.info('Gridder %r', gridder)

    job = self.load_job(job_id)

    combined = {}

    if not isinstance(attrs, list):
        attrs = [attrs,]

    for item in attrs:
        for key, value in item.iteritems():
            if key in combined:
                combined[key]['ingress'].append(value['ingress'])
            else:
                combined[key] = value

                combined[key]['ingress'] = [value['ingress'],]

    for key, value in cached.iteritems():
        if key in combined:
            combined[key].update(value)
        else:
            combined[key] = value

    output_name = '{}.nc'.format(uuid.uuid4())

    output_path = os.path.join(settings.WPS_LOCAL_OUTPUT_PATH, output_name)

    grid = None

    logger.info('Output path %r', output_path)

    with cdms2.open(output_path, 'w') as outfile:
        uris = sorted(combined.items(), key=lambda x: x[1]['base_units'])

        for item in uris:
            uri = item[0]

            uri_meta = item[1]

            if 'cached' in uri_meta:
                uri = uri_meta['cached']['path']

            logger.info('Opening %r', uri)

            with cdms2.open(uri) as infile:
                if 'cached' in uri_meta:
                    mapped = uri_meta['cached']['mapped']

                    chunked_axis = uri_meta['cached']['chunked_axis']

                    chunks = uri_meta['cached']['chunks']

                    for chunk in chunks:
                        mapped.update({ chunked_axis: chunk })

                        mapped_copy = mapped.copy()

                        time = mapped_copy.pop('time')

                        data = infile(var_name, time=time, **mapped_copy)

                        write_data(data, var_name, gridder, grid, base_units, outfile)
                else:
                    data = infile(var_name)

                    write_data(data, var_name, gridder, grid, base_units, outfile)

        logger.info('Final shape %r', outfile[var_name].shape)

    output_dap = settings.WPS_DAP_URL.format(filename=output_name)

    var = cwt.Variable(output_dap, var_name)

    logger.info('Marking job complete with output %r', var)

    job.succeeded(json.dumps(var.parameterize()))

    return combined

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
def average(self, attrs, operation, base_units):
    pass

@base.register_process('CDAT.sum', abstract=""" 
Computes the sum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def summation(self, attrs, operation, base_units):
    pass

@base.register_process('CDAT.max', abstract=""" 
Computes the maximum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def maximum(self, attrs, operation, base_units):
    pass

@base.register_process('CDAT.min', abstract="""
Computes the minimum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
                       """)
@base.cwt_shared_task()
def minimum(self, attrs, operation, base_units):
    pass

@base.register_process('CDAT.add', abstract="""
Compute the elementwise addition between two variables.
""")
@base.cwt_shared_task()
def add(self, attrs, operation, base_units):
    pass

@base.register_process('CDAT.subtract', abstract="""
Compute the elementwise subtraction between two variables.
""")
@base.cwt_shared_task()
def subtract(self, attrs, operation, base_units):
    pass

@base.register_process('CDAT.multiply', abstract="""
Compute the elementwise multiplication between two variables.
""")
@base.cwt_shared_task()
def multiply(self, attrs, operation, base_units):
    pass

@base.register_process('CDAT.divide', abstract="""
Compute the elementwise division between two variables.
""")
@base.cwt_shared_task()
def divide(self, attrs, operation, base_units):
    pass

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
