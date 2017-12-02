#! /usr/bin/env python

import os
import re

import cdms2
import cwt
import dask.array as da
from cdms2 import MV
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
Averages over one or more axes. Requires a parameter named "axes" whose value
is a "|" delimit list e.g. axes=lat|lon.
""")
@process.cwt_shared_task()
def average(self, parent_variables, variables, domains, operation, **kwargs):
    self.PUBLISH = process.ALL

    user, job = self.initialize(credentials=True, **kwargs)

    job.started()

    v, d, o = self.load(parent_variables, variables, domains, operation)

    if len(o.inputs) > 1:
        inputs = sort_inputs_by_time([v[x] for x in o.inputs if x in v])[0]
    else:
        inputs = v[o.inputs[0]]

    output_path = self.generate_output_path()

    axes = o.get_parameter('axes')

    if axes is None:
        axes = ['t']
    else:
        axes = axes.values

    domain = d.get(o.domain, None)

    with cdms2.open(inputs.uri) as input_file:
        uri = inputs.uri

        var_name = inputs.var_name

        files = {uri: input_file}

        file_var_map = {uri: inputs.var_name}

        domain_map = self.map_domain(files.values(), file_var_map, domain)

        cache_map = self.generate_cache_map(files, file_var_map, domain_map, job)

        partition_map = self.generate_partitions(domain_map, axes=axes)

        url = domain_map.keys()[0]

        # start grabbing data
        cache_file = None

        with cdms2.open(output_path, 'w') as outfile:
            if url in cache_map:
                try:
                    cache_file = cdms2.open(cache_map[url].local_path, 'w')
                except:
                    raise AccessError()

            temporal, spatial = partition_map[url]

            if isinstance(temporal, (list, tuple)):
                for time_slice in temporal:
                    data = files[url](var_name, time=time_slice, **spatial)

                    if any(x == 0 for x in data.shape):
                        raise InvalidShapeError('Data has shape {}'.format(data.shape))

                    if cache_file is not None:
                        cache_file.write(data, id=var_name)

                    for axis in axes:
                        if axis in ('time', 't'):
                            raise Exception('Average over time axis is not supported')

                        axis_index = data.getAxisIndex(axis)

                        if axis_index == -1:
                            raise Exception('Failed to map "{}" axis'.format(axis))

                        logger.info('Average over {}'.format(axis))

                        data = MV.average(data, axis=axis_index)

                    # write output data
                    outfile.write(data, id=var_name)
            elif isinstance(spatial, (list, tuple)):
                result = []

                for spatial_slice in spatial:
                    data = files[url](var_name, time=temporal, **spatial_slice)

                    if any(x == 0 for x in data.shape):
                        raise InvalidShapeError('Data has shape {}'.format(data.shape))

                    if cache_file is not None:
                        cache_file.write(data, id=var_name)

                    axis_index = data.getAxisIndex('time')

                    data = MV.average(data, axis=axis_index)

                    result.append(data)

                data = MV.concatenate(result)

                outfile.write(data, id=var_name)

        files[url].close()

        if cache_file is not None:
            cache_file.close()

            cache_map[url].size = os.stat(cache_map[url].local_path).st_size / 1073741824.0

            cache_map[url].save()

    output_url = self.generate_output_url(output_path, **kwargs)

    output_var = cwt.Variable(output_url, inputs.var_name, name=o.name)

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
