#! /usr/bin/env python

import os
import re

import cdms2
import cwt
import dask.array as da
from cdms2 import MV2 as MV
from celery.utils.log import get_task_logger

from wps.tasks import process

__ALL__ = [
    'subset',
    'aggregate',
    'cache_variable'
]

logger = get_task_logger('wps.tasks.cdat')

def sort_inputs_by_time(variables):
    input_dict = {}
    time_pattern = '.*_(\d+)-(\d+)\.nc'

    for v in variables:
        result = re.search(time_pattern, v.uri)

        start, _ = result.groups()

        input_dict[start] = v

    sorted_keys = sorted(input_dict.keys())

    return [input_dict[x] for x in sorted_keys]

@process.register_process('CDAT.subset', 'Subset a variable by provided domain. Supports regridding.')
@process.cwt_shared_task()
def subset(self, parent_variables, variables, domains, operation, **kwargs):
    self.PUBLISH = process.ALL

    user, job = self.initialize(credentials=True, **kwargs)

    job.started()

    v, d, o = self.load(parent_variables, variables, domains, operation)

    if len(o.inputs) > 1:
        inputs = sort_inputs_by_time([v[x] for x in o.inputs if x in v])[0]
    else:
        inputs = v[o.inputs[0]]

    grid, tool, method = self.generate_grid(o, v, d)

    def post_process(data):
        if grid is not None:
            data = data.regrid(grid, regridTool=tool, regridMethod=method)

        return data

    o.domain = d.get(o.domain, None)

    output_path = self.retrieve_variable([inputs], o.domain, job, post_process=post_process)

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs.var_name, name=o.name)

    return {o.name: output_var.parameterize()}

@process.register_process('CDAT.aggregate', 'Aggregate a variable over multiple files. Supports subsetting and regridding.')
@process.cwt_shared_task()
def aggregate(self, parent_variables, variables, domains, operation, **kwargs):
    self.PUBLISH = process.ALL

    user, job = self.initialize(credentials=True, **kwargs)

    job.started()

    v, d, o = self.load(parent_variables, variables, domains, operation)

    inputs = sort_inputs_by_time([v[x] for x in o.inputs if x in v])

    grid, tool, method = self.generate_grid(o, v, d)

    def post_process(data):
        if grid is not None:
            data = data.regrid(grid, regridTool=tool, regridMethod=method)

        return data

    o.domain = d.get(o.domain, None)

    output_path = self.retrieve_variable(inputs, o.domain, job, post_process=post_process)

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs[0].var_name, o.name)

    return {o.name: output_var.parameterize()}

@process.register_process('CDAT.average', """
Computes average over one or more axes. Requires a parameter named "axes" whose value
is a "|" delimit list e.g. axes=lat|lon.
""")
@process.cwt_shared_task()
def average(self, parent_variables, variables, domains, operation, **kwargs):
    return base_process(self, parent_variables, variables, domains, operation, MV.average, **kwargs)

@process.register_process('CDAT.max', """
Computes maximum over an axis. Requires a parameters named "axes" whos value is a
"|" delimit list e.g. axes=lat|lon.
""")
@process.cwt_shared_task()
def maximum(self, parent_variables, variables, domains, operation, **kwargs):
    return base_process(self, parent_variables, variables, domains, operation, MV.max, **kwargs)

@process.register_process('CDAT.min', """
Computes minimum over an axis. Requires a parameters named "axes" whos value is a
"|" delimit list e.g. axes=lat|lon.
""")
@process.cwt_shared_task()
def minimum(self, parent_variables, variables, domains, operation, **kwargs):
    return base_process(self, parent_variables, variables, domains, operation, MV.min, **kwargs)

@process.register_process('CDAT.sum', """
Computes sum over an axis. Requires a parameters named "axes" whos value is a
"|" delimit list e.g. axes=lat|lon.
""")
@process.cwt_shared_task()
def minimum(self, parent_variables, variables, domains, operation, **kwargs):
    return base_process(self, parent_variables, variables, domains, operation, MV.sum, **kwargs)

def base_process(self, parent_variables, variables, domains, operation, mv_process, **kwargs):
    self.PUBLISH = process.ALL

    user, job = self.initialize(credentials=True, **kwargs)

    job.started()

    v, d, o = self.load(parent_variables, variables, domains, operation)

    grid, tool, method = self.generate_grid(o, v, d)

    axes = o.get_parameter('axes')

    if axes is None:
        axes = ['time']
    else:
        axes = axes.values

    domain = d.get(o.domain, None)

    inputs = [v[x] for x in o.inputs if x in v]

    options = {
        'axes': axes,
        'grid': grid,
        'regridTool': tool,
        'regridMethod': method,
    }

    output_path = self.process_variable(inputs, domain, job, mv_process, **options)

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs[0].var_name, name=o.name)

    return {o.name: output_var.parameterize()}

@process.cwt_shared_task()
def cache_variable(self, parent_variables, variables, domains, operation, **kwargs):
    self.PUBLISH = process.RETRY | process.FAILURE

    user, job = self.initialize(credentials=True, **kwargs)

    job.started()

    variables.update(parent_variables)

    v, d, o = self.load(variables, domains, operation)

    o.domain = d.get(o.domain, None)

    inputs = sort_inputs_by_time([v[x] for x in o.inputs])

    output_path = self.retrieve_variable(inputs, o.domain, job, **kwargs)

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs[0].var_name, name=o.name)

    return {o.name: output_var.parameterize()}
