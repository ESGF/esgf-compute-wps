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

logger = get_task_logger(__name__)

__all__ = ['avg']

@register_process('CDAT.subset')
@shared_task(bind=True, base=CWTBaseTask)
def subset(self, variables, operations, domains, **kwargs):
    self.initialize(**kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.subset', o)

    var_name = op.inputs[0].var_name

    out_local_path = self.generate_local_output()

    with closing(cdms2.open(op.inputs[0].uri)) as inp:
        temporal, spatial = self.build_domain(inp, op.domain, var_name)

        tstart, tstop, tstep = temporal

        step = tstop - tstart if (tstop - tstart) < 200 else 200
            
        with closing(cdms2.open(out_local_path, 'w')) as out:
            for i in xrange(tstart, tstop, step):
                data = inp(var_name, time=slice(i, i+step, tstep), **spatial)

                out.write(data, id=var_name)

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()

@register_process('CDAT.aggregate')
@shared_task(bind=True, base=CWTBaseTask)
def aggregate(self, variables, operations, domains, **kwargs):
    self.initialize(**kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.aggregate', o)

    var_name = op.inputs[0].var_name

    out_local_path = self.generate_local_output()

    inputs = [cdms2.open(x.uri) for x in op.inputs]

    inputs = sorted(inputs, key=lambda x: x[var_name].getTime().units)

    with nested(*[closing(x) for x in inputs]) as inputs:
        with closing(cdms2.open(out_local_path, 'w')) as out:
            units = sorted([x[var_name].getTime().units for x in inputs])[0]

            for inp in inputs:
                data = inp(var_name)

                data.getTime().toRelativeTime(units)

                out.write(data, id=var_name)

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()

@register_process('CDAT.avg')
@shared_task(bind=True, base=CWTBaseTask)
def avg(self, variables, operations, domains, **kwargs):
    status = self.initialize(**kwargs)

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
