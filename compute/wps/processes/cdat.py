#! /usr/bin/env python

import os
import uuid
from contextlib import closing
from contextlib import nested

import cdms2
import cwt
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

    with closing(cdms2.open(op.inputs[0].uri)) as infile:
        domain_map = self.build_domain([infile], domains, var_name)

        temporal, spatial = domain_map[op.inputs[0].uri]

        tstart, tstop, tstep = temporal

        step = tstop - tstart if (tstop - tstart) < 200 else 200

        current = 0
        total = (tstop - tstart)

        grid, tool, method = self.generate_grid(op, v, d)
            
        with closing(cdms2.open(out_local_path.replace('https', 'http'), 'w')) as out:
            status.update('Subsetting {}'.format('w/regridding' if grid is not None else ''))

            for i in xrange(tstart, tstop, step):
                end = i + step

                if end > tstop:
                    end = tstop

                logger.info('Time slice {}:{}'.format(i, end))

                data = infile(var_name, time=slice(i, end, tstep), **spatial)

                if grid is not None:
                    data = data.regrid(grid, regridTool=tool, regridMethod=method)

                out.write(data, id=var_name)

                current += step

                status.update('Subsetting', percent=(current * 100) / total)

    status.update('Done subsetting')

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

    inputs = [cdms2.open(x.uri.replace('https', 'http')) for x in op.inputs]

    inputs = sorted(inputs, key=lambda x: x[var_name].getTime().units)

    with nested(*[closing(x) for x in inputs]) as inputs:
        domain_map = self.build_domain(inputs, domains, var_name)

        grid, tool, method = self.generate_grid(op, v, d)

        with closing(cdms2.open(out_local_path, 'w')) as out:
            units = sorted([x[var_name].getTime().units for x in inputs])[0]

            for infile in inputs:
                temporal, spatial = domain_map[infile.id]

                logger.info('Temporal {} Spatial {}'.format(temporal, spatial))

                tstart, tstop, tstep = temporal

                step = tstop - tstart if (tstop - tstart) < 200 else 200

                for i in xrange(tstart, tstop, step):
                    end = i + step

                    if end > tstop:
                        end = tstop

                    logger.info('Time slice {}:{}'.format(i, end))

                    data = infile(var_name, time=slice(i, end, tstep), **spatial)

                    data.getTime().toRelativeTime(units)

                    if grid is not None:
                        data = data.regrid(grid, regridTool=tool, regridMethod=method)

                    out.write(data, id=var_name)

    self.cleanup()

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()

@register_process('CDAT.avg')
@shared_task(bind=True, base=CWTBaseTask)
def avg(self, variables, operations, domains, **kwargs):
    status = self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.avg', o)

    var_name = op.inputs[0].var_name

    out_local_path = self.generate_local_output()

    inputs = [closing(cdms2.open(x.uri.replace('https', 'http')))
              for x in op.inputs]

    if op.domain is not None:
        domains['global'] = op.domain

    with nested(*inputs) as inputs:
        domain_map = self.build_domain([inputs[0]], domains, var_name)

        temporal, spatial = domain_map[inputs[0].id]

        logger.info('Temporal {} Spatial {}'.format(temporal, spatial))

        tstart, tstop, tstep = temporal

        step = tstop - tstart if (tstop - tstart) < 200 else 200

        with closing(cdms2.open(out_local_path, 'w')) as f:
            for i in xrange(tstart, tstop, step):
                data = sum(x(var_name, time=slice(i, i+step, tstep), **spatial)
                           for x in inputs) / len(inputs)

                f.write(data, id=var_name)

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()
