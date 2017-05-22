#! /usr/bin/env python

import os
import uuid
from contextlib import closing
from contextlib import nested

import cdms2
import cwt
import numpy as np
from celery import shared_task
from celery.utils.log import get_task_logger

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

    var_name = op.inputs[0].var_name

    out_local_path = self.generate_local_output()

    if op.domain is not None:
        domains['global'] = op.domain

    try:
        infile = cdms2.open(op.inputs[0].uri)    
    except cdms2.CDMSError:
        raise Exception('Failed to open file {}'.format(op.inputs[0].uri))

    if var_name not in infile.variables.keys():
        raise Exception('Variable {} is not present in {}'.format(var_name, op.inputs[0].uri))

    domain_map = self.build_domain([infile], domains, var_name)

    logger.info('Build domain map {}'.format(domain_map))

    cache_file, exists = self.cache_file(infile.id, domain_map)

    if exists:
        logger.info('File exists in cache')

        infile.close()

        infile = cdms2.open(cache_file)
    else:
        logger.info('File does not exist in cache')

        cache = cdms2.open(cache_file, 'w')

    with closing(infile) as infile:
        grid, tool, method = self.generate_grid(op, v, d)

        if exists:
            tstart = 0

            tstop = len(infile[var_name])

            tstep = 1

            spatial = {}
        else:
            temporal, spatial = domain_map[op.inputs[0].uri]

            tstart, tstop, tstep = temporal

        step = tstop - tstart if (tstop - tstart) < 200 else 200

        with closing(cdms2.open(out_local_path, 'w')) as out:
            for i in xrange(tstart, tstop, step):
                end = i + step

                if end > tstop:
                    end = tstop

                data = infile(var_name, time=slice(i, end, tstep), **spatial)

                if any(x == 0 for x in data.shape):
                    raise Exception('Read bad data, shape {}, check your domain'.format(data.shape))

                if not exists:
                    cache.write(data, id=var_name)

                if grid is not None:
                    data = data.regrid(grid, regridTool=tool, regridMethod=method)

                out.write(data, id=var_name)

            if not exists:
                cache.close()

    self.cleanup()

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()

@register_process('CDAT.aggregate')
@shared_task(bind=True, base=CWTBaseTask)
def aggregate(self, variables, operations, domains, **kwargs):
    self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.aggregate', o)

    var_name = op.inputs[0].var_name

    out_local_path = self.generate_local_output()

    if op.domain is not None:
        domains['global'] = op.domain

    try:
        inputs = [cdms2.open(x.uri.replace('https', 'http')) for x in op.inputs]
    except cdms2.CDMSError:
        raise Exception('Failed to open file {}'.format(op.inputs[0].uri))

    if not all(var_name in x.variables.keys() for x in inputs):
        raise Exception('Variable {} is not present in all input files'.format(var_name))

    inputs = sorted(inputs, key=lambda x: x[var_name].getTime().units)

    domain_map = self.build_domain(inputs, domains, var_name)

    logger.info('Generated domain map {}'.format(domain_map))

    cache_file, exists = self.cache_file(inputs[0].id, domain_map)

    if exists:
        logger.info('File exists in cache')

        for i in inputs:
            i.close()

        inputs = [cdms2.open(cache_file)]
    else:
        logger.info('File does not exist in cache')

        cache = cdms2.open(cache_file, 'w')

    with nested(*[closing(x) for x in inputs]) as inputs:
        grid, tool, method = self.generate_grid(op, v, d)

        with closing(cdms2.open(out_local_path, 'w')) as out:
            units = sorted([x[var_name].getTime().units for x in inputs])[0]

            for infile in inputs:
                if exists:
                    tstart = 0

                    tstop = len(infile[var_name])

                    tstep = 1

                    spatial = {}
                else:
                    temporal, spatial = domain_map[infile.id]

                    tstart, tstop, tstep = temporal

                step = tstop - tstart if (tstop - tstart) < 200 else 200

                for i in xrange(tstart, tstop, step):
                    end = i + step

                    if end > tstop:
                        end = tstop

                    data = infile(var_name, time=slice(i, end, tstep), **spatial)

                    if any(x == 0 for x in data.shape):
                        raise Exception('Read bad data, shape {}, check your domain'.format(data.shape))

                    data.getTime().toRelativeTime(units)

                    if not exists:
                        cache.write(data, id=var_name)

                    if grid is not None:
                        data = data.regrid(grid, regridTool=tool, regridMethod=method)

                    out.write(data, id=var_name)

            if not exists:
                cache.close()

    self.cleanup()

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()

@register_process('CDAT.avg')
@shared_task(bind=True, base=CWTBaseTask)
def avg(self, variables, operations, domains, **kwargs):
    # Currently all files are considered either cached or not cached,
    # we need to make this abit more robust to handle situations where
    # only a portion of the input files are cached.
    status = self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.avg', o)

    var_name = op.inputs[0].var_name

    out_local_path = self.generate_local_output()

    inputs = [cdms2.open(x.uri.replace('https', 'http')) for x in op.inputs]

    if op.domain is not None:
        domains['global'] = op.domain

    domain_map = self.build_domain(inputs, domains, var_name)

    logger.info('Generated domain map {}'.format(domain_map))

    cache_map = {}

    for key in domain_map.keys():
        cache_file, exists = self.cache_file(key, domain_map)

        if exists:
            logger.info('{} does exist in the cache'.format(key))

            found = [x for x in inputs if x.id == key][0]

            found.close()

            inputs.remove(found)

            fin = cdms2.open(cache_file)

            inputs.append(fin)

            domain_map[fin.id] = ((0, len(fin[var_name]), 1), {})
        else:
            logger.info('{} does not exist in the cache'.format(key))

            cache_map[key] = cdms2.open(cache_file, 'w')

    with nested(*[closing(x) for x in inputs]) as inputs:
        temporal, spatial = domain_map[inputs[0].id]
         
        tstart, tstop, tstep = temporal

        step = tstop - tstart if (tstop - tstart) < 200 else 200

        with closing(cdms2.open(out_local_path, 'w')) as f:
            if len(inputs) == 1:
                axes = op.get_parameter('axes', True)

                axes_ids = [inputs[0][var_name].getAxisIndex(x) for x in axes.values]

            for i in xrange(tstart, tstop, step):
                begin = i

                end = i + step

                if end > tstop:
                    end = tstop

                if len(inputs) == 1:
                    data = inputs[0](var_name, time=slice(begin, end, tstep), **spatial)

                    for axis in axes_ids:
                        data = np.average(data, axis=axis)

                    f.write(data, id=var_name)
                else:
                    for idx, inp in enumerate(inputs):
                        data = inp(var_name, time=slice(begin, end, tstep), **spatial)

                        if inp.id in cache_map:
                            cache_map[inp.id].write(data, id=var_name)

                        if idx == 0:
                            data_sum = data
                        else:
                            data_sum += data

                    data_sum /= len(inputs)

                    f.write(data_sum, id=var_name)

    for value in cache_map.values():
        value.close()

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()
