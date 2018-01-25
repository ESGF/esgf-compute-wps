#! /usr/bin/env python

import os
import re
import uuid

import cdms2
import cwt
import dask.array as da
from cdms2 import MV2 as MV
from celery.utils.log import get_task_logger

from wps import settings
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

@base.register_process('CDAT.subset', abstract='Subset a variable by provided domain. Supports regridding.')
@base.cwt_shared_task()
def subset(self, parent_variables, variables, domains, operation, user_id, job_id):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return retrieve_base(self, o, 1, user_id, job_id) 

@base.register_process('CDAT.aggregate', abstract='Aggregate a variable over multiple files. Supports subsetting and regridding.')
@base.cwt_shared_task()
def aggregate(self, parent_variables, variables, domains, operation, user_id, job_id):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return retrieve_base(self, o, None, user_id, job_id) 

def retrieve_base(self, operation, num_inputs, user_id, job_id):
    self.PUBLISH = base.ALL

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id)

    proc.job.started()

    output_name = '{}.nc'.format(str(uuid.uuid4()))

    output_path = os.path.join(settings.LOCAL_OUTPUT_PATH, output_name)

    try:
        with cdms2.open(output_path, 'w') as output_file:
            output_var_name = proc.retrieve(operation, num_inputs, output_file)
    except cdms2.CDMSError as e:
        raise base.AccessError(output_path, e.message)
    except WPSError:
        raise

    if settings.DAP:
        output_url = settings.DAP_URL.format(filename=output_name)
    else:
        output_url = settings.OUTPUT_URL.format(filename=output_name)

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
def average(self, parent_variables, variables, domains, operation, user_id, job_id):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return process_base(self, MV.average, 1, o, user_id, job_id)

@base.register_process('CDAT.sum', abstract=""" 
Computes the sum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def sum(self, parent_variables, variables, domains, operation, user_id, job_id):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return process_base(self, MV.sum, 1, o, user_id, job_id)

@base.register_process('CDAT.max', abstract=""" 
Computes the maximum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
""")
@base.cwt_shared_task()
def maximum(self, parent_variables, variables, domains, operation, user_id, job_id):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return process_base(self, MV.max, 1, o, user_id, job_id)

@base.register_process('CDAT.min', abstract="""
Computes the minimum over an axis. Requires singular parameter named "axes" 
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
                       """)
@base.cwt_shared_task()
def minimum(self, parent_variables, variables, domains, operation, user_id, job_id):
    _, _, o = self.load(parent_variables, variables, domains, operation)

    return process_base(self, MV.min, 1, o, user_id, job_id)

def process_base(self, process_func, num_inputs, operation, user_id, job_id):
    self.PUBLISH = base.ALL

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id)

    proc.job.started()

    output_name = '{}.nc'.format(str(uuid.uuid4()))

    output_path = os.path.join(settings.LOCAL_OUTPUT_PATH, output_name)

    try:
        with cdms2.open(output_path, 'w') as output_file:
            output_var_name = proc.process(operation, num_inputs, output_file, process_func)
    except cdms2.CDMSError as e:
        logger.exception('CDMS ERROR')
        raise base.AccessError(output_path, e)
    except WPSError:
        logger.exception('WPS ERROR')
        raise

    if settings.DAP:
        output_url = settings.DAP_URL.format(filename=output_name)
    else:
        output_url = settings.OUTPUT_URL.format(filename=output_name)

    output_variable = cwt.Variable(output_url, output_var_name).parameterize()

    return {operation.name: output_variable}

#@process.cwt_shared_task()
#def cache_variable(self, parent_variables, variables, domains, operation, **kwargs):
#    self.PUBLISH = process.RETRY | process.FAILURE
#
#    user, job = self.initialize(credentials=True, **kwargs)
#
#    job.started()
#
#    v, d, o = self.load(parent_variables, variables, domains, operation)
#
#    domain = d.get(o.domain, None)
#
#    inputs = sort_inputs_by_time([v[x] for x in o.inputs])
#
#    output_path = self.retrieve_variable(inputs, domain, job)
#
#    output_url = self.generate_output_url(output_path, **kwargs)
#
#    output_var = cwt.Variable(output_url, inputs[0].var_name, name=o.name)
#
#    return {o.name: output_var.parameterize()}
