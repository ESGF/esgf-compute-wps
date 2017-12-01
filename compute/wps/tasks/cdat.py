#! /usr/bin/env python

import re

import cdms2
import cwt
import dask.array as da
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

    # update variables with anything returned from a parent task
    variables.update(parent_variables)

    v, d, o = self.load(variables, domains, operation)

    inputs = sort_inputs_by_time([v[x] for x in o.inputs if x in v])

    grid, tool, method = self.generate_grid(o, v, d)

    def post_process(data):
        if grid is not None:
            data = data.regrid(grid, regridTool=tool, regridMethod=method)

        return data

    o.domain = d.get(o.domain, None)

    output_path = self.retrieve_variable([inputs[0]], o.domain, job, post_process=post_process)

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs[0].var_name)

    return {o.name: output_var.parameterize()}

@process.register_process('CDAT.aggregate', 'Aggregate a variable over multiple files. Supports subsetting and regridding.')
@process.cwt_shared_task()
def aggregate(self, parent_variables, variables, domains, operation, **kwargs):
    self.PUBLISH = process.ALL

    user, job = self.initialize(credentials=True, **kwargs)

    job.started()

    variables.update(parent_variables)

    v, d, o = self.load(variables, domains, operation)

    inputs = sort_inputs_by_time([v[x] for x in o.inputs if x in v])

    grid, tool, method = self.generate_grid(o, v, d)

    def post_process(data):
        if grid is not None:
            data = data.regrid(grid, regridTool=tool, regridMethod=method)

        return data

    o.domain = d.get(o.domain, None)

    output_path = self.retrieve_variable(inputs, o.domain, job, post_process=post_process)

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs[0].var_name)

    return {o.name: output_var.parameterize()}

#@process.register_process('CDAT.average', 'Averages over axes, requires a parameter "axes" set to the axes to average over.')
@process.cwt_shared_task()
def average(self, variables, operations, domains, **kwargs):
    self.PUBLISH = process.ALL

    job, status = self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.average', o)

    inputs = sort_inputs_by_time(op.inputs)

    output_path = self.generate_output_path()

    axes = op.get_parameter('axes')

    if axes is None:
        axes = ['t']
    else:
        axes = axes.values

    # For the moment if given multiple files we'll just processes the first
    if len(inputs) > 1:
        inputs = inputs[0]

    input_file = cdms2.open(inputs.uri)

    input_dict = {input_file.id: inputs.var_name}

    logger.info(input_dict)

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs.var_name)

    return output_var.parameterize()

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
