#! /usr/bin/env python

import os
import uuid

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
@shared_task(base=CWTBaseTask)
def subset(variables, operations, domains, **kwargs):
    cwd = kwargs.get('cwd')

    if cwd is not None:
        os.chdir(cwd)

    d = dict((x, cwt.Domain.from_dict(y)) for x, y in domains.iteritems())

    v = dict((x, cwt.Variable.from_dict(y)) for x, y in variables.iteritems())

    for var in v.values():
        var.resolve_domains(d)

    o = dict((x, cwt.Process.from_dict(y)) for x, y in operations.iteritems())

    op_by_id = lambda x: [y for y in o.values() if y.identifier == x][0]

    op = op_by_id('CDAT.subset')

    op.resolve_inputs(v, o)

    out_file_name = '{}.nc'.format(uuid.uuid4())

    out_file_path = '{}/{}'.format(settings.OUTPUT_LOCAL_PATH, out_file_name)

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

    logger.info('Domain {}'.format(dom_kw))

    out = cdms2.open(out_file_path, 'w')

    logger.info(inp[var_name].shape)

    data = inp(var_name, **dom_kw)

    out.write(data, id=var_name)

    logger.info(out[var_name].shape)

    out.close()

    out_var = cwt.Variable(settings.OUTPUT_URL.format(file_name=out_file_name), var_name)

    return out_var.parameterize()

@register_process('CDAT.aggregate')
@shared_task(base=CWTBaseTask)
def aggregate(variables, operations, domains, **kwargs):
    cwd = kwargs.get('cwd')

    if cwd is not None:
        os.chdir(cwd)

    v = dict((x, cwt.Variable.from_dict(y)) for x, y in variables.iteritems())

    o = dict((x, cwt.Process.from_dict(y)) for x, y in operations.iteritems())

    d = dict((x, cwt.Domain.from_dict(y)) for x, y in domains.iteritems())

    op_by_id = lambda x: [y for y in o.values() if y.identifier == x][0]

    op = op_by_id('CDAT.aggregate')

    op.resolve_inputs(v, o)

    sort_inputs = sorted(op.inputs, key=lambda x: x.uri.split('/')[-1])

    var_name = sort_inputs[0].var_name

    inputs = [cdms2.open(x.uri, 'r') for x in sort_inputs]

    out_file_name = '{}.nc'.format(uuid.uuid4())

    out_file_path = '{}/{}'.format(settings.OUTPUT_LOCAL_PATH, out_file_name)

    out = cdms2.open(out_file_path, 'w')

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

    out.close()

    out_var = cwt.Variable(settings.OUTPUT_URL.format(file_name=out_file_name), var_name)

    return out_var.parameterize()

@register_process('CDAT.avg')
@shared_task(base=CWTBaseTask)
def avg(variables, operations, domains, **kwargs):
    cwd = kwargs.get('cwd')

    if cwd is not None:
        os.chdir(cwd)

    v = dict((x, cwt.Variable.from_dict(y)) for x, y in variables.iteritems())

    o = dict((x, cwt.Process.from_dict(y)) for x, y in operations.iteritems())

    d = dict((x, cwt.Domain.from_dict(y)) for x, y in domains.iteritems())

    op_by_id = lambda x: [y for y in o.values() if y.identifier == x][0]

    op = op_by_id('CDAT.avg')

    op.resolve_inputs(v, o)

    var_name = op.inputs[0].var_name

    logger.info([x.uri for x in op.inputs])

    inputs = [cdms2.open(x.uri.replace('https', 'http')) for x in op.inputs]

    n = inputs[0][var_name].shape[0]

    out_file_name = '{}.nc'.format(uuid.uuid4())

    out_file_path = '{}/{}'.format(settings.OUTPUT_LOCAL_PATH, out_file_name)

    out = cdms2.open(out_file_path, 'w')

    for i in xrange(0, n, 200):
        data = sum(x(var_name, slice(i, i+200)) for x in inputs) / len(inputs)

        out.write(data, id=var_name)

    # Clean up
    for x in inputs:
        x.close()

    out.close()

    out_var = cwt.Variable(settings.OUTPUT_URL.format(file_name=out_file_name), var_name)

    return out_var.parameterize()
