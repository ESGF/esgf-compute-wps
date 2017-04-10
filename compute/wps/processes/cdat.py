#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import uuid
from collections import deque
from contextlib import closing

import cdms2
import cwt
from celery import group
from celery import shared_task
from celery.utils.log import get_task_logger

from wps import settings

logger = get_task_logger(__name__)

dummy_wps = cwt.WPS('')

@shared_task()
def workflow(data_inputs):
    operations, domains, inputs = cwt.WPS.parse_data_inputs(data_inputs)

    find_by_id = lambda x: [y for y in operations if y.identifier == x][0]

    workflow = find_by_id('CDAT.workflow')

    temp_inputs = dict((x.name, x) for x in inputs)

    temp_operations = dict((x.name, x) for x in operations)

    workflow.resolve_inputs(temp_inputs, temp_operations)

    logger.info('Workflow inputs {}'.format(workflow.inputs))

    child_tasks = []

    for p in workflow.inputs:
        new_data_inputs = dummy_wps.prepare_data_inputs(p, inputs, domains)

        if p.identifier == 'CDAT.avg':
            child_tasks.append(avg.s(new_data_inputs))

    if len(child_tasks) > 0:
        logger.info('Calling child tasks {}'.format(child_tasks))

        child_group = group(child_tasks)()

        import time

        while not child_group.ready():
            time.sleep(1)

        logger.info(child_group)

    return 'FAIL'

@shared_task()
def avg(data_inputs):
    operations, domains, inputs = cwt.WPS.parse_data_inputs(data_inputs)

    op = operations[0]

    temp_inputs = dict((x.name, x) for x in inputs)

    temp_operations = dict((x.name, x) for x in operations)

    op.resolve_inputs(temp_inputs, temp_operations)

    if len(op.inputs) < 1:
        raise Exception('Expecting a single input file')

    child_tasks = []

    for p in op.inputs:
        if not isinstance(p, cwt.Variable):
            new_data_inputs = dummy_wps.prepare_data_inputs(p, inputs, domains)

            if p.identifier == 'CDAT.avg':
                child_tasks.append(avg.s(new_data_inputs))

    if len(child_tasks) > 0:
        logger.info('Calling children')

        child_group = group(child_tasks)()

        import time

        while not child_group.ready():
            time.sleep(1)

        logger.info('HELP!! {}'.format(child_group))

    input_data = [x.uri for x in op.inputs if isinstance(x, cwt.Variable)]

    logger.info(input_data)

    with closing(cdms2.open(op.inputs[0].uri, 'r')) as f:
        logger.info('Reading data from %s', op.inputs[0].uri)        

        var_name = op.inputs[0].var_name

        n = f[var_name].shape[0]

    file_name = '{}.nc'.format(uuid.uuid4())

    file_path = '{0}/{1}'.format(settings.OUTPUT_LOCAL_PATH, file_name)

    logger.info('Local file path {}'.format(file_path))

    inps = [cdms2.open(x.uri, 'r') for x in op.inputs]

    n_inps = len(inps)

    with closing(cdms2.open(file_path, 'w')) as fout:
        for i in xrange(0, n, 200):
            for idx, v in enumerate(inps):
                data = v(var_name, slice(i, i+200))

                if idx == 0:
                    out = data
                else:
                    out[:] += data[:]

            out /= n_inps

            fout.write(out, id=var_name)

    if settings.DAP:
        url = settings.DAP_URL.format(file_name=file_name)
    else:
        url = settings.OUTPUT_URL.format(file_name=file_name)

    logger.info('Output url {}'.format(url))

    return url
