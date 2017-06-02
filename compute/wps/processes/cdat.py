#! /usr/bin/env python

import os
import uuid
from contextlib import closing
from contextlib import nested

import cdms2
import cwt
import dask.array as da
import numpy as np
from celery import shared_task
from celery.utils.log import get_task_logger
from dask import delayed

from wps import models
from wps import settings
from wps.processes import CWTBaseTask
from wps.processes import register_process

logger = get_task_logger('wps.processes.cdat')

__all__ = ['avg']

@register_process('CDAT.subset')
@shared_task(bind=True, base=CWTBaseTask)
def subset(self, variables, operations, domains, **kwargs):
    status = self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.subset', o)

    input_var = op.inputs[0]

    var_name = input_var.var_name

    try:
        input_file = cdms2.open(input_var.uri)
    except cdms2.CDMSError:
        raise Exception('Failed to open file {}'.format(input_var.uri))

    if var_name not in input_file.variables.keys():
        raise Exception('Variable {} is not present in {}'.format(var_name, input_var.uri))

    logger.info('Processing input {}'.format(input_file.id))

    temporal, spatial = self.map_domain(input_file, var_name, op.domain)

    cache, exists = self.check_cache(input_var.uri, var_name, temporal, spatial)

    if exists:
        spatial = {}

        input_file = cache

        temporal = slice(0, len(input_file[var_name]), 1)

        logger.info('Adjusted time slice to {}'.format(temporal))

    out_local_path = self.generate_local_output()

    with closing(input_file) as input_file:
        grid, tool, method = self.generate_grid(op, v, d)

        tstart, tstop, tstep = temporal.start, temporal.stop, temporal.step

        diff = tstop - tstart

        step = diff if diff < 200 else 200

        with closing(cdms2.open(out_local_path, 'w')) as out:
            logger.info('Writing to output {}'.format(out_local_path))

            for i in xrange(tstart, tstop, step):
                end = i + step

                if end > tstop:
                    end = tstop

                time_slice = slice(i, end, tstep)

                logger.info('Read chunk {}'.format(time_slice))

                data = input_file(var_name, time=time_slice, **spatial)

                if any(x == 0 for x in data.shape):
                    raise Exception('Read bad data, shape {}, check your domain'.format(data.shape))

                if not exists:
                    logger.info('Writing to cache file {}'.format(cache.id))

                    cache.write(data, id=var_name)

                if grid is not None:
                    before = data.shape

                    data = data.regrid(grid, regridTool=tool, regridMethod=method)

                    logger.info('Regridded {} => {}'.format(before, data.shape))

                out.write(data, id=var_name)

    if not exists:
        logger.info('Closing cache file.')

        cache.close()

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()

@register_process('CDAT.aggregate')
@shared_task(bind=True, base=CWTBaseTask)
def aggregate(self, variables, operations, domains, **kwargs):
    self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.aggregate', o)

    var_name = reduce(lambda x, y: x if x == y else None, [x.var_name for x in op.inputs])

    if var_name is None:
        raise Exception('Input variable names do not match.')

    inputs = {}

    try:
        for i in op.inputs:
            inputs[i.uri] = cdms2.open(i.uri)
    except cdms2.CDMSError as e:
        # Cleanup files
        for v in inputs.values():
            v.close()

        raise Exception(e)

    if not all(var_name in x.variables.keys() for x in inputs.values()):
        raise Exception('Variable {} is not present in all input files'.format(var_name))

    logger.info('Processing inputs {}'.format(inputs.keys()))

    domain_map = self.map_domain_multiple(inputs.values(), var_name, op.domain)

    cache_map = {}
   
    for file_path in domain_map.keys():
        temporal, spatial = domain_map[file_path]

        if temporal is None:
            inputs[file_path].close()

            inputs.pop(file_path)

            domain_map.pop(file_path)

            continue

        cache, exists = self.check_cache(file_path, var_name, temporal, spatial)

        if exists:
            inputs[file_path].close()
            
            inputs[file_path] = cache

            domain_map[cache.id] = domain_map.pop(file_path)

            n = len(inputs[file_path][var_name])

            domain = (slice(0, n, 1), {})

            logger.info('Adjusting domain to {}'.format(domain))
        else:
            cache_map[file_path] = cache

    logger.info(domain_map)

    out_local_path = self.generate_local_output()

    with nested(*[closing(x) for x in inputs.values()]) as inputs:
        grid, tool, method = self.generate_grid(op, v, d)

        with closing(cdms2.open(out_local_path, 'w')) as out:
            logger.info('Writing to output {}'.format(out_local_path))

            units = sorted([x[var_name].getTime().units for x in inputs])[0]

            logger.info('Using base unit "{}"'.format(units))

            inputs = sorted(inputs, key=lambda x: x[var_name].getTime().units)

            for inp in inputs:
                logger.info('Processing input {}'.format(inp.id))

                temporal, spatial = domain_map[inp.id]

                tstart, tstop, tstep = temporal.start, temporal.stop, temporal.step

                diff = tstop - tstart

                step = diff if diff < 200 else 200

                for i in xrange(tstart, tstop, step):
                    end = i + step

                    if end > tstop:
                        end = tstop

                    time_slice = slice(i, end, tstep)

                    logger.info('Reading chunk {}'.format(time_slice))

                    data = inp(var_name, time=slice(i, end, tstep), **spatial)

                    if any(x == 0 for x in data.shape):
                        raise Exception('Read bad data, shape {}, check your domain'.format(data.shape))

                    data.getTime().toRelativeTime(units)

                    if inp.id in cache_map:
                        logger.info('Writing to cache file {}'.format(cache_map[inp.id].id))

                        cache_map[inp.id].write(data, id=var_name)

                    if grid is not None:
                        before = data.shape

                        data = data.regrid(grid, regridTool=tool, regridMethod=method)

                        logger.info('Regridder {} => {}'.format(before, data.shape))

                    out.write(data, id=var_name)

    logger.info('Closing up cache files {}'.format([x.id for x in cache_map.values()]))

    for v in cache_map.values():
        v.close()

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()

@register_process('CDAT.avg')
@shared_task(bind=True, base=CWTBaseTask)
def avg(self, variables, operations, domains, **kwargs):
    status = self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.avg', o)

    out_local_path = self.generate_local_output()

    if len(op.inputs) == 1:
        input_var = op.inputs[0]

        var_name = input_var.var_name

        input_file = self.cache_input(input_var, op.domain)

        axes = op.get_parameter('axes', True)

        if axes is None:
            raise Exception('axes parameter was not defined')

        with input_file as input_file:
            axis_indexes = [input_file[var_name].getAxisIndex(x) for x in axes.values]

            if any(x == -1 for x in axis_indexes):
                truth = zip(axes.values, [True if x != -1 else False
                                          for x in axis_indexes])

                raise Exception('An axis does not exist {}'.format(truth))

            shape = input_file[var_name].shape

            chunk = [shape[0] / 10] + list(shape[1:])

            chunk = tuple(chunk)

            mean = da.from_array(input_file[var_name], chunks=chunk)

            for axis in axis_indexes:
                mean = mean.mean(axis=axis)

            result = mean.compute()

            axes = [x for x in input_file.axes.values() if x.id not in axes.values and x.id != 'bound']

            with cdms2.open(out_local_path, 'w') as output_file:
                output_file.write(result, id=var_name, axes=axes)
    else:
        raise Exception('Average between multiple files is not supported yet.')

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()
