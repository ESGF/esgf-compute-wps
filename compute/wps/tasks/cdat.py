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

@base.register_process('CDAT.subset', aliases='CDAT.regrid', abstract='Subset a variable by provided domain. Supports regridding.')
@base.cwt_shared_task()
def subset(self, parent_variables, variables, domains, operation, user_id, job_id):
    self.PUBLISH = base.ALL

    v, d, o = self.load(parent_variables, variables, domains, operation)

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id)

    output_name = '{}.nc'.format(str(uuid.uuid4()))

    output_path = os.path.join(settings.LOCAL_OUTPUT_PATH, output_name)

    try:
        with file_manager.FileManager(o.inputs) as fm, cdms2.open(output_path, 'w') as output_file:
            proc.retrieve(fm, o, 1, output_file)
    except cdms2.CDMSError as e:
        raise base.AccessError(output_path, e.message)
    except WPSError:
        raise

    if settings.DAP:
        output_url = settings.DAP_URL.format(filename=output_name)
    else:
        output_url = settings.OUTPUT_URL.format(filename=output_name)

    output_variable = cwt.Variable(output_url, 'tas').parameterize()

    return {o.name: output_variable}

#@process.register_process('CDAT.aggregate', 'Aggregate a variable over multiple files. Supports subsetting and regridding.')
#@process.cwt_shared_task()
#def aggregate(self, parent_variables, variables, domains, operation, **kwargs):
#    self.PUBLISH = process.ALL
#
#    user, job = self.initialize(credentials=True, **kwargs)
#
#    job.started()
#
#    v, d, o = self.load(parent_variables, variables, domains, operation)
#
#    inputs = sort_inputs_by_time([v[x] for x in o.inputs if x in v])
#
#    grid, tool, method = self.generate_grid(o, v, d)
#
#    def post_process(data):
#        if grid is not None:
#            data = data.regrid(grid, regridTool=tool, regridMethod=method)
#
#        return data
#
#    o.domain = d.get(o.domain, None)
#
#    output_path = self.retrieve_variable(inputs, o.domain, job, post_process=post_process)
#
#    output_url = self.generate_output_url(output_path, **kwargs)
#
#    output_var = cwt.Variable(output_url, inputs[0].var_name, name=o.name)
#
#    return {o.name: output_var.parameterize()}
#
#@process.register_process('CDAT.average', """
#Computes average over one or more axes. Requires a parameter named "axes" whose value
#is a "|" delimit list e.g. axes=lat|lon.
#""")
#@process.cwt_shared_task()
#def average(self, parent_variables, variables, domains, operation, **kwargs):
#    return base_process(self, parent_variables, variables, domains, operation, MV.average, **kwargs)
#
#@process.register_process('CDAT.max', """
#Computes maximum over an axis. Requires a parameters named "axes" whos value is a
#"|" delimit list e.g. axes=lat|lon.
#""")
#@process.cwt_shared_task()
#def maximum(self, parent_variables, variables, domains, operation, **kwargs):
#    return base_process(self, parent_variables, variables, domains, operation, MV.max, **kwargs)
#
#@process.register_process('CDAT.min', """
#Computes minimum over an axis. Requires a parameters named "axes" whos value is a
#"|" delimit list e.g. axes=lat|lon.
#""")
#@process.cwt_shared_task()
#def minimum(self, parent_variables, variables, domains, operation, **kwargs):
#    return base_process(self, parent_variables, variables, domains, operation, MV.min, **kwargs)
#
#@process.register_process('CDAT.sum', """
#Computes sum over an axis. Requires a parameters named "axes" whos value is a
#"|" delimit list e.g. axes=lat|lon.
#""")
#@process.cwt_shared_task()
#def sum(self, parent_variables, variables, domains, operation, **kwargs):
#    return base_process(self, parent_variables, variables, domains, operation, MV.sum, **kwargs)
#
#@process.register_process('CDAT.diff', """
#Computes element-wise difference between two files.""")
#@process.cwt_shared_task()
#def difference(self, parent_variables, variables, domains, operation, **kwargs):
#    proc = lambda data: MV.subtract(data[0], data[1])
#
#    return base_process(self, parent_variables, variables, domains, operation, proc, num_inputs=2, **kwargs)
#
#def base_process(self, parent_variables, variables, domains, operation, mv_process, num_inputs=1, **kwargs):
#    self.PUBLISH = process.ALL
#
#    user, job = self.initialize(credentials=True, **kwargs)
#
#    job.started()
#
#    v, d, o = self.load(parent_variables, variables, domains, operation)
#
#    grid, tool, method = self.generate_grid(o, v, d)
#
#    axes = o.get_parameter('axes')
#
#    if axes is None:
#        axes = ['time']
#    else:
#        axes = axes.values
#
#    domain = d.get(o.domain, None)
#
#    inputs = [v[x] for x in o.inputs if x in v]
#
#    options = {
#        'axes': axes,
#        'grid': grid,
#        'regridTool': tool,
#        'regridMethod': method,
#    }
#
#    output_path = self.process_variable(inputs, domain, job, mv_process, num_inputs, **options)
#
#    output_url = self.generate_output_url(output_path, **kwargs)
#
#    output_var = cwt.Variable(output_url, inputs[0].var_name, name=o.name)
#
#    return {o.name: output_var.parameterize()}
#
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
