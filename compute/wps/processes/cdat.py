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

    with closing(cdms2.open(op.inputs[0].uri)) as inp:
        domain = { inp.id: op.domain }

        temporal, spatial = self.build_domain([inp], domain, var_name)

        tstart, tstop, tstep = temporal[0]

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

                data = inp(var_name, time=slice(i, end, tstep), **spatial[0])

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

    domain = {}

    if op.domain is not None:
        domain['global'] = op.domain
    
    for i in op.inputs:
        if i.domains is None:
            domain[i.uri] = None
        else:
            domain[i.uri] = i.domains[0]

    inputs = [cdms2.open(x.uri.replace('https', 'http')) for x in op.inputs]

    inputs = sorted(inputs, key=lambda x: x[var_name].getTime().units)

    grid, tool, method = self.generate_grid(op, v, d)

    with nested(*[closing(x) for x in inputs]) as inputs:
        with closing(cdms2.open(out_local_path, 'w')) as out:
            units = sorted([x[var_name].getTime().units for x in inputs])[0]

            gtemporal, gspatial = self.build_domain(inputs, domain, var_name)

            for inp, temporal, spatial in zip(inputs, gtemporal, gspatial):
                tstart, tstop, tstep = temporal

                step = tstop - tstart if (tstop - tstart) < 200 else 200

                for i in xrange(tstart, tstop, step):
                    end = i + step

                    if end > tstop:
                        end = tstop

                    logger.info('Time slice {}:{}'.format(i, end))

                    data = inp(var_name, time=slice(i, end, tstep), **spatial)

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

    with nested(*inputs) as inputs:
        temporal, spatial = self.build_domain(inputs[0], op.domain, var_name)

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
