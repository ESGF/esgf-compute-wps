#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import uuid
from contextlib import closing

import cdms2
import cwt
from celery import shared_task
from celery.utils.log import get_task_logger

from wps import settings

logger = get_task_logger(__name__)

@shared_task()
def avg(data_inputs):
    operations, _, _ = cwt.WPS.parse_data_inputs(data_inputs)

    op = operations[0]

    if len(op.inputs) < 1:
        raise Exception('Expecting a single input file')

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
