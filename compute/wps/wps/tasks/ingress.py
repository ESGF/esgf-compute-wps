#! /usr/bin/env python

from future import standard_library
standard_library.install_aliases()
from builtins import str
import contextlib
import datetime
import hashlib
import os
import uuid
from urllib.parse import urlparse

import cdms2
from cdms2 import MV2
from django import db
from django.conf import settings
from celery.utils import log

from wps import helpers
from wps import metrics
from wps import models
from wps import WPSError
from wps.context import OperationContext
from wps.tasks import base
from wps.tasks import preprocess

logger = log.get_task_logger('wps.tasks.ingress')

@base.cwt_shared_task()
def ingress_cleanup(self, context):
    for input in context.inputs:
        for file_path in (input.ingress + input.process):
            try:
                os.remove(file_path)
            except:
                pass

            logger.info('Removed %r', file_path)

    return context

def write_cache_file(entry, input, context):
    data = []
    chunk_axis = None
    chunk_axis_index = None

    entry.local_path = entry.new_output_path()

    with context.new_output(entry.local_path) as outfile:
        for _, _, chunk in input.chunks_ingress(context):
            if chunk_axis is None:
                chunk_axis_index = chunk.getAxisIndex(input.chunk_axis)

                chunk_axis = chunk.getAxis(chunk_axis_index)

            if chunk_axis.isTime():
                outfile.write(chunk, id=input.variable.var_name)
            else:
                data.append(chunk)

        if chunk_axis is not None and not chunk_axis.isTime():
            data = MV2.concatenate(data, axis=chunk_axis_index)

            outfile.write(data, id=input.variable.var_name)

    try:
        size = entry.set_size()
    except db.IntegrityError:
        # Handle case where cache files are written at same time
        pass
    else:
        metrics.WPS_DATA_CACHE_WRITE.inc(size)

@base.cwt_shared_task()
def ingress_cache(self, context):
    if context.operation.get_parameter('intermediate') is None:
        for input in context.inputs:
            if input.mapped is None or input.cache is not None:
                continue

            entry = preprocess.check_cache_entries(input, context)

            if entry is not None:
                continue

            kwargs = {
                'url': input.variable.uri,
                'variable': input.variable.var_name,
                'dimensions': helpers.encoder(input.mapped),
            }

            entry = models.Cache(**kwargs)

            write_cache_file(entry, input, context)

    return context

@base.cwt_shared_task()
def ingress_chunk(self, context, index):
    for input_index, input in enumerate(context.sorted_inputs()):
        if input.is_cached or input.mapped is None:
            logger.info('Skipping %r either cached or not included', input.variable.uri)

            continue

        for _, chunk_index, chunk in input.chunks(index, context):
            start = datetime.datetime.now()

            ingress_filename = '{}_{:08}_{:08}.nc'.format(str(context.job.id),
                                                          input_index, chunk_index)

            ingress_path = context.gen_ingress_path(ingress_filename)

            with context.new_output(ingress_path) as outfile:
                outfile.write(chunk, id=input.variable.var_name)

            input.ingress.append(ingress_path)

            elapsed = datetime.datetime.now() - start

            self.status('Ingressed {!r} bytes in {!r} seconds', chunk.nbytes, elapsed.total_seconds())

    return context
