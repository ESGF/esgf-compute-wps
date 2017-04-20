#! /usr/bin/env python

import uuid

import cdms2
import cwt
from celery import shared_task
from celery.utils.log import get_task_logger

from wps import models
from wps import settings
from wps.processes import register_process

logger = get_task_logger(__name__)

__all__ = ['avg']

@register_process('CDAT.aggregate')
@shared_task
def aggregate(variables, operations, domains, **kwargs):
    if isinstance(variables, dict):
        variables = [variables]
    
    var_objects = [cwt.Variable.from_dict(x) for x in variables]

    v = dict((x.name, x) for x in var_objects)

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
@shared_task
def avg(variables, operations, domains, **kwargs):
    if isinstance(variables, dict):
        variables = [variables]
    
    var_objects = [cwt.Variable.from_dict(x) for x in variables]

    v = dict((x.name, x) for x in var_objects)

    o = dict((x, cwt.Process.from_dict(y)) for x, y in operations.iteritems())

    d = dict((x, cwt.Domain.from_dict(y)) for x, y in domains.iteritems())

    op_by_id = lambda x: [y for y in o.values() if y.identifier == x][0]

    op = op_by_id('CDAT.avg')

    inputs = [v[x] for x in op.inputs]

    var_name = inputs[0].var_name

    inputs = [cdms2.open(x.uri) for x in inputs]

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
