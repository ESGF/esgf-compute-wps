#! /usr/bin/env python

import contextlib
import datetime
import hashlib
import os
import psutil
import uuid
from urlparse import urlparse

import cdms2
from cdms2 import MV2
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

    with context.new_output(entry.local_path) as outfile:
        for _, _, chunk in input.chunks_ingress(context):
            if chunk_axis is None:
                chunk_axis_index = chunk.getAxisIndex(input.chunk_axis)

                chunk_axis = chunk.getAxis(chunk_axis_index)

            if chunk_axis.isTime():
                outfile.write(chunk, id=input.variable.var_name)
            else:
                data.append(chunk)

        if not chunk_axis.isTime():
            data = MV2.concatenate(data, axis=chunk_axis_index)

            outfile.write(data, id=input.variable.var_name)

    entry.set_size()

@base.cwt_shared_task()
def ingress_cache(self, context):
    for input in context.inputs:
        entry = preprocess.check_cache_entries(input, context)

        if entry is not None:
            continue

        mapped = input.mapped.copy()

        mapped.update({ 'var_name': input.variable.var_name })

        uid = '{}:{}'.format(input.variable.uri, input.variable.var_name)

        kwargs = {
            'uid': hashlib.sha256(uid).hexdigest(),
            'url': input.variable.uri,
            'dimensions': helpers.encoder(mapped),
        }

        entry = models.Cache(**kwargs)

        write_cache_file(entry, input, context)

    return context

@base.cwt_shared_task()
def ingress_chunk(self, context, index):
    for input_index, input in enumerate(context.sorted_inputs()):
        if input.is_cached:
            logger.info('Skipping %r already cached', input.variable.uri)

            continue

        for _, chunk_index, chunk in input.chunks(index, context):
            ingress_filename = '{}_{:08}_{:08}.nc'.format(str(context.job.id),
                                                          input_index, chunk_index)

            ingress_path = context.gen_ingress_path(ingress_filename)

            with context.new_output(ingress_path) as outfile:
                outfile.write(chunk, id=input.variable.var_name)

            input.ingress.append(ingress_path)

    return context
