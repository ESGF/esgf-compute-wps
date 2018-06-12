#! /usr/bin/env python

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

from wps import models
from wps import WPSError
from wps.tasks import base
from wps.tasks import process
from wps.tasks import file_manager

__ALL__ = [
    'subset',
    'aggregate',
    'cache_variable'
]

logger = get_task_logger('wps.tasks.cdat')

OUTPUT = cwt.wps.process_output_description('output', 'output', 'application/json')

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

@base.register_process('CDAT.regrid', abstract="""
Regrids a variable to designated grid. Required parameter named "gridder".
""")
@base.cwt_shared_task()
def regrid(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    def validate(op):
        op.get_parameter('gridder', True)

    return retrieve_base(self, o, None, user_id, job_id, process_id, validate=validate, **kwargs) 

@base.register_process('CDAT.subset', abstract='Subset a variable by provided domain. Supports regridding.')
@base.cwt_shared_task()
def subset(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    def validate(op):
        if op.domain is None:
            raise WPSError('Missing required domain')

    return retrieve_base(self, o, None, user_id, job_id, process_id, validate=validate, **kwargs) 

@base.register_process('CDAT.aggregate', abstract='Aggregate a variable over multiple files. Supports subsetting and regridding.')
@base.cwt_shared_task()
def aggregate(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return retrieve_base(self, o, None, user_id, job_id, process_id, **kwargs) 

def retrieve_base(self, operation, num_inputs, user_id, job_id, process_id, validate=None, **kwargs):
    """ Configures and executes a retrieval process.

    Sets up a retrieval process by initializing the process with the user and
    job id. It then marks the job as started.

    The process is execute and a path to the results is returned using a 
    cwt.Variable instance.

    The validate function should match the following signature where operation
    is a cwt.Process instance and the function returns True or False.

    def validate(operation):
        return True

    Args:
        operation: A cwt.Process instance complete with inputs and domain set to
            instances.
        num_inputs: An integer value of the number of inputs to process.
        user_id: A user integer id.
        job_id: A job integer id.
        validate: A function to be called that validates if all required details
            are available.

    Returns:
         A dict mapping operations name to a cwt.Variable instance.

         {'sub_out': cwt.Variable('http://test.com/some/data', 'tas')}

    Raises:
        AccessError: An error occurred accessing a NetCDF file.
        WPSError: An error occurred during processing.
    """
    self.PUBLISH = base.ALL

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id, process_id)

    proc.job.started()

    if validate is not None:
        validate(operation)

    output_name = '{}.nc'.format(str(uuid.uuid4()))

    output_path = os.path.join(settings.WPS_LOCAL_OUTPUT_PATH, output_name)

    try:
        with cdms2.open(output_path, 'w') as output_file:
            output_var_name = proc.retrieve_data(operation, num_inputs, output_file, **kwargs)
    except cdms2.CDMSError as e:
        raise base.AccessError(output_path, e.message)
    except WPSError:
        raise

    if settings.WPS_DAP:
        output_url = settings.WPS_DAP_URL.format(filename=output_name)
    else:
        output_url = settings.WPS_OUTPUT_URL.format(filename=output_name)

    output_variable = cwt.Variable(output_url, output_var_name).parameterize()

    return {operation.name: output_variable}

#@base.register_process('CDAT.max', abstract=""" 
#Computes the maximum over an axis. Requires singular parameter named "axes" 
#whose value will be used to process over. The value should be a "|" delimited
#string e.g. 'lat|lon'.
#""")
#@base.cwt_shared_task()
#def maximum(self, parent_variables, variables, domains, operation, user_id, job_id):
#    return process_base(self, MV.max, 1, parent_variables, variables, domains, operation, user_id, job_id)

@base.register_process('CDAT.average', abstract=""" 
Computes the average over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def average(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    kwargs['axes'] = o.get_parameter('axes', True).values[0]

    return process_base(self, MV.average, 1, o, user_id, job_id, process_id, **kwargs)

@base.register_process('CDAT.sum', abstract=""" 
Computes the sum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def summation(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    kwargs['axes'] = o.get_parameter('axes', True).values[0]

    return process_base(self, MV.sum, 1, o, user_id, job_id, process_id, **kwargs)

@base.register_process('CDAT.max', abstract=""" 
Computes the maximum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def maximum(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    kwargs['axes'] = o.get_parameter('axes', True).values[0]

    return process_base(self, MV.max, 1, o, user_id, job_id, process_id, **kwargs)

@base.register_process('CDAT.min', abstract="""
Computes the minimum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
                       """)
@base.cwt_shared_task()
def minimum(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    kwargs['axes'] = o.get_parameter('axes', True).values[0]

    return process_base(self, MV.min, 1, o, user_id, job_id, process_id, **kwargs)

@base.register_process('CDAT.add', abstract="""
Compute the elementwise addition between two variables.
""")
@base.cwt_shared_task()
def add(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return process_base(self, MV.add, 2, o, user_id, job_id, process_id, **kwargs)

@base.register_process('CDAT.subtract', abstract="""
Compute the elementwise subtraction between two variables.
""")
@base.cwt_shared_task()
def subtract(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return process_base(self, MV.subtract, 2, o, user_id, job_id, process_id, **kwargs)

@base.register_process('CDAT.multiply', abstract="""
Compute the elementwise multiplication between two variables.
""")
@base.cwt_shared_task()
def multiply(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return process_base(self, MV.multiply, 2, o, user_id, job_id, process_id, **kwargs)

@base.register_process('CDAT.divide', abstract="""
Compute the elementwise division between two variables.
""")
@base.cwt_shared_task()
def divide(self, parent_variables, variables, domains, operation, user_id, job_id, process_id, **kwargs):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return process_base(self, MV.divide, 2, o, user_id, job_id, process_id, **kwargs)

def process_base(self, process_func, num_inputs, operation, user_id, job_id, process_id, **kwargs):
    """ Configures and executes a process.

    Sets up the process by initializing it with the user_id and job_id, marks
    the job as started. The processes is then executed and a path to the output
    is returned in a cwt.Variable instance.

    The process_func is a method that will take in an list of data chunks, 
    process them and return a single data chunk. This output data chunk will be
    written to the output file.

    Args:
        process_func: A function that will be passed the data to be processed.
        num_inputs: An integer value of the number of inputs to process.
        operation: A cwt.Process instance, complete with inputs and domain.
        user_id: An integer user id.
        job_id: An integer job id.

    Returns:
        A dict mapping operation name to a cwt.Variable instance.

        {'max': cwt.Variable('http://test.com/some/data', 'tas')}

    Raises:
        AccessError: An error occurred acessing a NetCDF file.
        WPSError: An error occurred processing the data.
    """
    self.PUBLISH = base.ALL

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id, process_id)

    proc.job.started()

    output_name = '{}.nc'.format(str(uuid.uuid4()))

    output_path = os.path.join(settings.WPS_LOCAL_OUTPUT_PATH, output_name)

    try:
        with cdms2.open(output_path, 'w') as output_file:
            output_var_name = proc.process_data(operation, num_inputs, output_file, process_func, **kwargs)
    except cdms2.CDMSError as e:
        logger.exception('CDMS ERROR')
        raise base.AccessError(output_path, e)
    except WPSError:
        logger.exception('WPS ERROR')
        raise

    if settings.WPS_DAP:
        output_url = settings.WPS_DAP_URL.format(filename=output_name)
    else:
        output_url = settings.WPS_OUTPUT_URL.format(filename=output_name)

    output_variable = cwt.Variable(output_url, output_var_name).parameterize()

    return {operation.name: output_variable}

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
