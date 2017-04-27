#! /usr/bin/env python

import os
import uuid
from contextlib import closing

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

def int_or_float(value):
    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        return None

@register_process('CDAT.subset')
@shared_task(bind=True, base=CWTBaseTask)
def subset(self, variables, operations, domains, **kwargs):
    self.initialize(**kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.subset')

    out_name, out_path = self.create_output()

    var_name = op.inputs[0].var_name

    # Only process first inputs
    inp = cdms2.open(op.inputs[0].uri, 'r')

    # Only process first domain
    try:
        dom = op.inputs[0].domains[0]
    except IndexError:
        raise Exception('Input has not domain defined.')

    dom_kw = {}

    for dim in dom.dimensions:
        args = None

        if dim.crs == cwt.INDICES:
            # Single slice or range
            if dim.start == dim.end:
                args = slice(dim.start, dim.end+1, dim.step)
            else:
                args = slice(dim.start, dim.end, dim.step)
        elif dim.crs == cwt.VALUES:
            if dim.start == dim.end:
                args = dim.start
            else:
                if dim.name == 'time':
                    args = (str(dim.start), str(dim.end))
                else:
                    axis_index = inp[var_name].getAxisIndex(dim.name)

                    axis = inp[var_name].getAxis(axis_index)

                    args = axis.mapInterval((int_or_float(dim.start), int_or_float(dim.end)))
        else:
            raise Exception('Unknown CRS {}'.format(dim.crs))

        dom_kw[dim.name] = args

    with closing(cdms2.open(out_file_path, 'w')) as out:
        data = inp(var_name, **dom_kw)

        out.write(data, id=var_name)

    out_var = cwt.Variable(settings.OUTPUT_URL.format(file_name=out_file_name), var_name)

    return out_var.parameterize()

@register_process('CDAT.aggregate')
@shared_task(bind=True, base=CWTBaseTask)
def aggregate(self, variables, operations, domains, **kwargs):
    self.initialize(**kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.aggregate')

    sort_inputs = sorted(op.inputs, key=lambda x: x.uri.split('/')[-1])

    var_name = sort_inputs[0].var_name

    inputs = [cdms2.open(x.uri, 'r') for x in sort_inputs]

    out_name, out_path = self.create_output()

    with closing(cdms2.open(out_file_path, 'w')) as out:
        units = None

        for a in inputs:
            n = a[var_name].getTime()

            if units is None:
                units = n.units

            for b in xrange(0, len(n), 200):
                data = a(var_name, time=slice(b, b+200))

                data.getTime().toRelativeTime(units)

                out.write(data, id=var_name)

            a.close()

    out_var = cwt.Variable(settings.OUTPUT_URL.format(file_name=out_file_name), var_name)

    return out_var.parameterize()

@register_process('CDAT.avg')
@shared_task(bind=True, base=CWTBaseTask)
def avg(self, variables, operations, domains, **kwargs):
    self.initialize(**kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('CDAT.avg', o)

    var_name = op.inputs[0].var_name

    inputs = [cdms2.open(x.uri.replace('https', 'http')) for x in op.inputs]

    n = inputs[0][var_name].shape[0]

    out_name, out_path = self.create_output()

    with closing(cdms2.open(out_path, 'w')) as out:
        for i in xrange(0, n, 200):
            data = sum(x(var_name, slice(i, i+200)) for x in inputs) / len(inputs)

            out.write(data, id=var_name)

    for x in inputs:
        x.close()

    out_var = cwt.Variable(settings.OUTPUT_URL.format(file_name=out_name), var_name)

    return out_var.parameterize()
