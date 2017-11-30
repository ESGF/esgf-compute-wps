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
def subset(self, variables, operations, domains, **kwargs):
    self.PUBLISH = process.ALL

    user, job = self.initialize(credentials=True, **kwargs)

    job.started()

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.subset', o)

    inputs = sort_inputs_by_time(op.inputs)

    grid, tool, method = self.generate_grid(op, v, d)

    def post_process(data):
        if grid is not None:
            data = data.regrid(grid, regridTool=tool, regridMethod=method)

        return data

    output_path = self.retrieve_variable([inputs[0]], op.domain, job, post_process=post_process)

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs[0].var_name)

    return output_var.parameterize()

@process.register_process('CDAT.aggregate', 'Aggregate a variable over multiple files. Supports subsetting and regridding.')
@process.cwt_shared_task()
def aggregate(self, variables, operations, domains, **kwargs):
    self.PUBLISH = process.ALL

    user, job = self.initialize(credentials=True, **kwargs)

    job.started()

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.aggregate', o)

    inputs = sort_inputs_by_time(op.inputs)

    grid, tool, method = self.generate_grid(op, v, d)

    def post_process(data):
        if grid is not None:
            data = data.regrid(grid, regridTool=tool, regridMethod=method)

        return data

    output_path = self.retrieve_variable(inputs, op.domain, job, post_process=post_process)

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs[0].var_name)

    return output_var.parameterize()

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
def cache_variable(self, identifier, variables, domains, operations, **kwargs):
    self.PUBLISH = process.RETRY | process.FAILURE

    user, job = self.initialize(kwargs.get('user_id'), kwargs.get('job_id'), credentials=True)

    job.started()

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id(identifier, o)

    output_path = self.retrieve_variable([op.inputs[0]], op.domain, job, **kwargs)

    output_url = self.generate_output_url(output_path, **kwargs)

    op.inputs = [cwt.Variable(output_url, op.inputs[0].var_name)]

    data_inputs = cwt.WPS('').prepare_data_inputs(op, [], op.domain)

    return data_inputs
