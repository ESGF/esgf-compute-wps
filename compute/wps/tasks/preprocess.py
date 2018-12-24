#! /usr/bin/env python

import collections
import contextlib
import copy
import hashlib
import json
import math
import os

import cwt
import cdms2
import requests
from celery.utils.log import get_task_logger
from django.conf import settings

from wps import helpers
from wps import metrics
from wps import models
from wps import WPSError
from wps.context import OperationContext
from wps.tasks import base

logger = get_task_logger('wps.tasks.preprocess')

@base.cwt_shared_task()
def merge(self, contexts):
    context = OperationContext.merge_inputs(contexts)

    return context

def axis_size(data):
    if isinstance(data, slice):
        return ((data.stop - data.start) / data.step) * 4

    return data

@base.cwt_shared_task()
def generate_chunks(self, context):
    axes = context.operation.get_parameter('axes')

    if axes is None:
        process_axis = set()
    else:
        process_axis = set(axes.values)

    logger.info('Process axes %r', process_axis)

    for input in context.inputs:
        order = input.mapped_order

        # Find axes that we can chunk over
        file_axes = set(order) - process_axis

        # Select the lowest order axis that's available
        try:
            chunk_axis = reduce(lambda x, y: x if order.index(x) <
                                        order.index(y) else y, file_axes)
        except TypeError:
            # Default to time axis?
            chunk_axis = None

        logger.info('Selected %r axis for chunking', chunk_axis)

        if input.is_cached:
            mapped = input.cache_mapped()
        else:
            mapped = input.mapped

        if chunk_axis is not None and len(mapped) > 0:
            non_chunk_axes = [mapped[x] for x in (set(order) - set([chunk_axis]))]

            logger.info('Non chunk axes %r', non_chunk_axes)

            # Reduce won't convert if array length is 1
            if len(non_chunk_axes) > 1:
                chunk_size = reduce(lambda x, y: axis_size(x) * axis_size(y),
                                    non_chunk_axes)
            else:
                chunk_size = axis_size(non_chunk_axes[0])

            logger.info('Chunk size %r', chunk_size)

            chunk_per_worker = int(settings.WORKER_MEMORY / 2 / chunk_size)

            logger.info('Chunk per worker %r', chunk_per_worker)

            if chunk_per_worker == 0:
                raise WPSError('A single chunk cannot fit it memory, consider'
                               ' subsetting the data further.')

            chunk_axis_slice = mapped[chunk_axis]

            for start in range(chunk_axis_slice.start, chunk_axis_slice.stop,
                               chunk_per_worker):
                input.chunk.append(slice(start, min(start + chunk_per_worker,
                                                     chunk_axis_slice.stop),
                                          chunk_axis_slice.step))

        input.chunk_axis = chunk_axis

        logger.info('Generated %r chunks over %r axis', len(input.chunk),
                    input.chunk_axis)

    return context

def check_cache_entries(input, context):
    cache_entries = models.Cache.objects.filter(
        url=input.variable.uri,
        variable=input.variable.var_name
    )

    for entry in cache_entries:
        logger.info('Evaluating cached file %r', entry.local_path)

        try:
            entry.validate()
        except Exception as e:
            logger.info('Removing invalid cache file: %r', e)

            entry.delete()

            continue

        if entry.is_superset(input.mapped):
            logger.info('Found valid entry in %r', entry.local_path)

            return entry

    logger.info('Found no valid cache entries for %r', input.variable.uri)

    return None

@base.cwt_shared_task()
def check_cache(self, context):
    for input in context.inputs:
        if input.mapped is None:
            logger.info('Skipping input %r', input.variable.uri)

            continue

        input.cache = check_cache_entries(input, context)

    return context

def map_axis_interval(axis, start, stop):
    try:
        map = axis.mapInterval((start, stop))
    except Exception:
        raise WPSError('Unabled to map interval {!r} to {!r}', start, stop)

    return map

def map_axis(axis, dimension, units):
    if axis.isTime() and units is not None:
        axis = axis.clone()

        axis.toRelativeTime(str(units))

    if dimension is None or dimension.crs == cwt.INDICES:
        step = 1 if dimension is None else helpers.int_or_float(dimension.step)

        selector = slice(0, len(axis), step)

        metrics.WPS_DOMAIN_CRS.labels(cwt.INDICES).inc()
    elif dimension.crs == cwt.VALUES:
        start = helpers.int_or_float(dimension.start)

        stop = helpers.int_or_float(dimension.end)

        step = helpers.int_or_float(dimension.step)

        map = map_axis_interval(axis, start, stop)

        selector = slice(map[0], map[1], step)

        metrics.WPS_DOMAIN_CRS.labels(cwt.VALUES).inc()
    elif dimension.crs == cwt.TIMESTAMPS:
        step = helpers.int_or_float(dimension.step)
        
        map = map_axis_interval(axis, dimension.start, dimension.end)

        selector = slice(map[0], map[1], step)

        metrics.WPS_DOMAIN_CRS.labels(cwt.TIMESTAMPS).inc()
    else:
        raise WPSError('Unknown CRS {!r}', dimension.crs)

    return selector

@base.cwt_shared_task()
def map_domain(self, context):
    for input in context.inputs:
        with input.open(context.user) as var:
            axes = var.getAxisList()

            input.mapped_order = [x.id for x in axes]

            if context.domain is None:
                try:
                    input.mapped = dict((x.id, map_axis(x, None, context.units)) 
                                        for x in axes)
                except Exception:
                    input.mapped = {}
            else:
                user_dim = set([x.name for x in context.domain.dimensions])

                file_dim = set(input.mapped_order)

                if not user_dim <= file_dim:
                    raise WPSError('User defined domain is invalid, {!r} are'
                                   ' missing from the file',
                                   ', '.join(user_dim-file_dim))

                file_dim -= user_dim

                for name in (user_dim | file_dim):
                    dim = context.domain.get_dimension(name) if name in user_dim else None

                    axis_index = var.getAxisIndex(name)

                    axis = var.getAxis(axis_index).clone()

                    try:
                        input.mapped[name] = map_axis(axis, dim, context.units)
                    except Exception:
                        input.mapped = {}

        logger.info('Mapped domain to %r', input.mapped)

    return context

@base.cwt_shared_task()
def base_units(self, context):
    units = []

    for input in context.inputs:
        with input.open(context.user) as var:
            time = var.getTime()

            if time is not None:
                input.first = time[0]

                input.units = time.units

                units.append(input.units)

    try:
        context.units = sorted(units)[0]
    except IndexError:
        pass

    return context

@base.cwt_shared_task()
def filter_inputs(self, context, index):
    indices = self.generate_indices(index, len(context.inputs))

    context.inputs = [context.inputs[x] for x in indices]

    return context
