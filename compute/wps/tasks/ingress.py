#! /usr/bin/env python

import contextlib
import datetime
import hashlib
import os
import psutil
import uuid
from urlparse import urlparse

import cdms2
from cdms2 import MV2 as MV
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
        if len(input.ingress) == 0:
            continue 

        for file_path in input.ingress:
            try:
                os.remove(file_path)
            except:
                pass

    return context

def write_cache_file(entry, input, context):
    with context.new_output(entry.local_path) as outfile:
        for _, chunk in input.chunks():
            outfile.write(chunk, id=input.variable.var_name)

    entry.set_size()

@base.cwt_shared_task()
def ingress_cache(self, context):
    for input in context.inputs:
        entry = preprocess.check_cache_entries(input, context)

        if entry is not None or len(input.ingress) == 0:
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
    base = 0

    for input in context.sorted_inputs():
        if input.cache_uri is not None:
            continue

        indices = self.generate_indices(index, len(input.chunk))

        for index, chunk in input.chunks(context, indices):
            local_index = base + index

            local_filename = 'data_{}_{:08}.nc'.format(str(context.job.id), local_index)

            local_path = context.gen_ingress_path(local_filename)

            with context.new_output(local_path) as outfile:
                outfile.write(chunk, id=input.variable.var_name)

            input.ingress.append(local_path)

        base = len(input.chunk)

    return context
