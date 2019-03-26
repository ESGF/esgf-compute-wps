#! /usr/bin/env python

from __future__ import division
from builtins import str
from builtins import range
from past.utils import old_div
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
from functools import reduce

logger = get_task_logger('wps.tasks.preprocess')

@base.cwt_shared_task()
def merge(self, contexts):
    context = OperationContext.merge_inputs(contexts)

    logger.info('Merged contexts to %r', context)

    return context

def axis_size(data):
    if isinstance(data, slice):
        return (old_div((data.stop - data.start), data.step)) * 4

    return data

@base.cwt_shared_task()
def generate_chunks(self, context):
    """ Generate chunks.

    Args:
        context (OperationContext): Current context.

    Returns:
        Updated context.
    """
    axes = context.operation.get_parameter('axes')

    try:
        process_axis = set(axes.values)
    except AttributeError:
        process_axis = set()

    logger.info('Process axes %r', process_axis)

    for input in context.inputs:
        order = input.mapped_order

        logger.info('Using ordering %r', order)

        # Find axes that we can chunk over
        file_axes = set(order) - process_axis

        logger.info('File axes %r', file_axes)

        # Select the lowest order axis that's available
        try:
            chunk_axis = reduce(lambda x, y: x if order.index(x) <
                                        order.index(y) else y, file_axes)
        except TypeError:
            # Default to time axis?
            chunk_axis = None

        logger.info('Selected %r axis for chunking', chunk_axis)

        # Determine which mapping to use.
        if input.is_cached:
            mapped = input.cache_mapped()

            logger.info('Cached mapping')
        else:
            mapped = input.mapped

            logger.info('Normal mapping')

        logger.info('Using mapping %r', mapped)

        if chunk_axis is not None and mapped is not None:
            # Collect non chunk axis slices
            non_chunk_axes = [mapped[x] for x in (set(order) - set([chunk_axis]))]

            logger.info('Non chunk axes %r', non_chunk_axes)

            # Reduce won't convert if array length is 1
            if len(non_chunk_axes) > 1:
                chunk_size = reduce(lambda x, y: axis_size(x) * axis_size(y),
                                    non_chunk_axes)
            else:
                chunk_size = axis_size(non_chunk_axes[0])

            logger.info('Chunk size %r', chunk_size)

            chunk_per_worker = int(old_div(settings.WORKER_MEMORY, old_div(chunk_size, 2)))

            logger.info('Chunk per worker %r', chunk_per_worker)

            if chunk_per_worker == 0:
                raise WPSError('A single chunk cannot fit it memory, consider'
                               ' subsetting the data further.')

            chunk_axis_slice = mapped[chunk_axis]

            logger.info('Chunk axis %r', chunk_axis_slice)

            # Generate actual chunks
            for start in range(chunk_axis_slice.start, chunk_axis_slice.stop,
                               chunk_per_worker):
                input.chunk.append(slice(start, min(start + chunk_per_worker,
                                                     chunk_axis_slice.stop),
                                          chunk_axis_slice.step))

                logger.info('Chunk %r', input.chunk[-1])

        input.chunk_axis = chunk_axis

        self.status('Generated {!r} chunks over {!r} axis for {!r}', len(input.chunk), input.chunk_axis, input.filename)

    return context

def check_cache_entries(input, context):
    """ Checks for cached version of the input.

    Args:
        input (cwt.Variable): Variable thats being searched for.
        context (OperationContext): Current context:

    Returns:
        A cache entry if found otherwise None.
    """
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
    """ Map start/stop to axis.

    Args:
        axis (cdms2.Axis): Axis to be mapped.
        start (int): Start value.
        stop (int): Stop value.

    Returns:
        A tuple with the mapping.
    """
    try:
        map = axis.mapInterval((start, stop))
    except Exception:
        raise WPSError('Unabled to map interval {!r} to {!r}', start, stop)

    return map

def map_axis_indices(axis, dimension):
    """ Maps axis to indices.

    Args:
        axis (cdms2.Axis): Axis to be mapped.
        dimension (cwt.Dimension): The dimension to map.

    Returns:
        A selector for cdms2.
    """
    try:
        start = helpers.int_or_float(dimension.start)
    except AttributeError:
        start = 0

    try:
        stop = helpers.int_or_float(dimension.end)
    except AttributeError:
        stop = len(axis)
    finally:
        stop = min(stop, len(axis))

    try:
        step = helpers.int_or_float(dimension.step)
    except AttributeError:
        step = 1

    selector = slice(start, stop, step)

    if axis.isTime() and dimension is not None:
        dimension.start = 0

        dimension.end -= selector.stop

    metrics.WPS_DOMAIN_CRS.labels(cwt.INDICES).inc()

    return selector

def map_axis_values(axis, dimension):
    """ Map axis to values.
    
    Args:
        axis (cdms2.Axis): Axis to be mapped.
        dimension (cwt.Dimension): The dimension to map.

    Returns:
        A selector for cdms2.
    """
    start = helpers.int_or_float(dimension.start)

    stop = helpers.int_or_float(dimension.end)

    step = helpers.int_or_float(dimension.step)

    map = map_axis_interval(axis, start, stop)

    selector = slice(map[0], map[1], step)

    metrics.WPS_DOMAIN_CRS.labels(cwt.VALUES).inc()

    return selector

def map_axis_timestamps(axis, dimension):
    """ Map axis to timestamps.

    Args:
        axis (cdms2.Axis): Axis to be mapped.
        dimension (cwt.Dimension): The dimension to map.

    Returns:
        A selector for cdms2.
    """
    step = helpers.int_or_float(dimension.step)
    
    map = map_axis_interval(axis, dimension.start, dimension.end)

    selector = slice(map[0], map[1], step)

    metrics.WPS_DOMAIN_CRS.labels(cwt.TIMESTAMPS).inc()

    return selector

def map_axis(axis, dimension, units):
    """ Maps an axis to a dimension.

    Args:
        axis (cdms2.axis): Axis to be mapped.
        dimension (cwt.Dimension): Dimension being mapped to.
        units (str): Units to be used if a time axis is being mapped.

    Returns:
        A slice that will be used as a cdms2 selector.
    """
    if axis.isTime() and units is not None:
        axis = axis.clone()

        axis.toRelativeTime(str(units))

    if dimension is None or dimension.crs == cwt.INDICES:
        selector = map_axis_indices(axis, dimension)
    elif dimension.crs == cwt.VALUES:
        selector = map_axis_values(axis, dimension)
    elif dimension.crs == cwt.TIMESTAMPS:
        selector = map_axis_timestamps(axis, dimension)
    else:
        raise WPSError('Unknown CRS {!r}', dimension.crs)

    return selector

def merge_dimensions(context, file_dimensions):
    """ Merge user and file dimensions.

    Args:
        context (OperationContext): Current context.
        file_dimensions (list,tuple): Dimension names in file.

    Returns:
        Complete list of dimension names.
    """
    try:
        user_dim = [x.name for x in context.domain.dimensions]
    except AttributeError:
        user_dim = []

    user_dim = set(user_dim)

    logger.info('User dimensions %r', user_dim)

    file_dim = set(file_dimensions)

    logger.info('File dimensions %r', file_dim)

    # Check that user dimensions are a subset of the file dimensions
    if not user_dim <= file_dim:
        raise WPSError('User defined axes {!r} were not found in the file', 
                       ', '.join(user_dim-file_dim))

    # Isolate dimensions not defined by the user
    file_dim -= user_dim

    # Return union of user and file dimensions
    return user_dim | file_dim

@base.cwt_shared_task()
def map_domain(self, context):
    """ Maps a domain.

    Args:
        context (OperationContext): Current operation context.

    Returns:
        An updated operation context.
    """
    for input in context.inputs:
        self.status('Mapping {!r}', input.filename)

        with input.open(context.user) as var:
            axes = var.getAxisList()

            input.mapped_order = [x.id for x in axes]

            logger.info('Axis mapped order %r', input.mapped_order)

            dimensions = merge_dimensions(context, input.mapped_order)

            logger.info('Merge dimensions %r', dimensions)

            for name in dimensions:
                try:
                    dim = context.domain.get_dimension(name)
                except AttributeError:
                    dim = None

                axis_index = var.getAxisIndex(name)

                if axis_index == -1:
                    raise WPSError('Axis {!r} was not found in remote file',
                                   name)

                axis = var.getAxis(axis_index).clone()

                try:
                    input.mapped[name] = map_axis(axis, dim, context.units)
                #except WPSError:
                #    raise
                except Exception:
                    input.mapped = None

                    break

                logger.info('Mapped %r to %r', name, input.mapped[name])

    return context

@base.cwt_shared_task()
def base_units(self, context):
    """ Gets base units for OperationContext.
    
    Args:
        context (OperationContext): Current operation context.

    Returns:
        An OperationContext whose inputs have their time units
        filled out.
    """
    units = []

    logger.info('Determining base units')

    for input in context.inputs:
        with input.open(context.user) as var:
            time = var.getTime()

            if time is not None:
                input.first = time[0]

                input.units = time.units

                units.append(input.units)

                logger.info('%r units: %r first: %r', input.filename, input.units,
                            input.first)
            else:
                logger.info('Skipping %r', input.filename)

    try:
        context.units = sorted(units)[0]
    except IndexError:
        pass

    self.status('Setting units to {!r}', context.units)

    return context

@base.cwt_shared_task()
def filter_inputs(self, context, index):
    """ Filters context inputs.

    Args:
        context (OperationContext): Current operation context.
        index (int): The index to filter on.

    Returns:
        An OperationContext whose inputs have been filtered.
    """
    logger.debug('Index %r Length %r Workers %r', index, len(context.inputs),
                 settings.WORKER_PER_USER)

    indices = [x for x in range(index, len(context.inputs),
                                settings.WORKER_PER_USER)]

    logger.info('Filtering indices %r', indices)

    context.inputs = [context.inputs[x] for x in indices]

    return context
