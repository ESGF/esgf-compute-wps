#! /usr/bin/env python

import contextlib
import hashlib

import cwt
import cdms2
from celery.utils.log import get_task_logger

from wps import helpers
from wps import models
from wps import WPSError
from wps.tasks import base
from wps.tasks import credentials

logger = get_task_logger('wps.tasks.preprocess')

def get_axis_slice(domain, axis_name):
    dimension = domain.get_dimension(axis_name)

    return [
        helpers.int_or_float(dimension.start),
        helpers.int_or_float(dimension.end),
    ]

def get_uri(infile):
    return infile.uri

def get_axis(infile, var_name, axis):
    variable = infile[var_name]

    axis_index = variable.getAxisIndex(axis)

    if axis_index == -1:
        raise WPSError('Axis "{id}" was not found in "{url}"', id=axis, url=infile.url)

    return variable.getAxis(axis_index)

def get_axis_list(infile, var_name, exclude=None):
    if exclude is None:
        exclude = []

    return [x for x in infile[var_name].getAxisList() if x.id not in exclude]

def map_remaining_axes(infile, var_name, domain, exclude):
    mapped_axes = {}

    logger.info('Mapping remaining axes excluding %r', exclude)

    for axis in get_axis_list(infile, var_name, exclude):
        dimension = domain.get_dimension(axis.id)

        if dimension is None:
            shape = axis.shape[0]

            dimension = cwt.Dimension(axis.id, 0, shape, cwt.INDICES)

            logger.info('Found %r in file but not domain', axis.id)

        if dimension.crs == cwt.VALUES:
            try:
                interval = axis.mapInterval((dimension.start, dimension.end))
            except TypeError:
                raise WPSError('Failed to map axis "{id}"', id=dimension.name)

            mapped_axes[axis.id] = slice(interval[0], interval[1])
        elif dimension.crs == cwt.INDICES:
            mapped_axes[axis.id] = slice(dimension.start, dimension.end)
        else:
            raise WPSError('Unable to handle crs "{name}"', name=dimension.crs.name)

        logger.info('Mapped %r to %r', dimension, mapped_axes[axis.id])

    return mapped_axes

@base.cwt_shared_task()
def check_cache(self, attrs, uri):
    axis_map = attrs.get('axis_map')

    if axis_map is None:
        attrs['cached'] = None

        return attrs

    var_name = attrs['var_name']

    uid = '{}:{}'.format(uri, attrs['var_name'])

    uid_hash = hashlib.sha256(uid).hexdigest()

    cache_entries = models.Cache.objects.filter(uid=uid_hash)

    logger.info('Found %r cache entries for %r', len(cache_entries), uid_hash)

    domain = {}

    for name in sorted(axis_map.keys()):
        value = axis_map[name]

        if name in ('time', 't', 'z'):
            domain['temporal'] = value
        else:
            if 'spatial' in domain:
                domain['spatial'][name] = value
            else:
                domain['spatial'] = {
                    name: value
                }

    logger.info('Built domain %r', domain)

    for x in xrange(cache_entries.count()):
        entry = cache_entries[x]

        if not entry.valid:
            logger.info('Cache entry %r invalid, removing...', entry.url)
            
            entry.delete()
            
            continue

        if entry.is_superset(domain):
            logger.info('Cache entry %r is valid and a superset', entry.url)

            attrs['cached'] = entry.url

            break

    if 'cached' not in attrs:
        logger.info('No cache entry was found')

        attrs['cached'] = None

    return attrs

@base.cwt_shared_task()
def map_axis_indices(self, uris, var_name, axis, domain, user_id):
    try:
        user = models.User.objects.get(pk=user_id)
    except models.User.DoesNotExist:
        raise WPSError('User "{id}" does not exist', id=user_id)

    credentials.load_certificate(user)

    logger.info('Mapping domain %r', domain)

    axis_slice = get_axis_slice(domain, axis)

    logger.info('Mapping axis slice %r', axis_slice)

    attrs = {
        'var_name': var_name,
        'axis': axis,
        'axis_slice': axis_slice[:],
    }

    file_axis_map = {}

    with contextlib.nested(*[cdms2.open(x) for x in uris]) as infiles:
        for x in infiles:
            axis_map = {}

            file_name = get_uri(x)

            logger.info('Processing %r', file_name)

            file_axis = get_axis(x, var_name, axis)

            shape = file_axis.shape[0]

            if axis_slice[0] > shape:
                logger.info('%r not included in domain', file_name)

                axis_slice[0] -= shape

                axis_slice[1] -= shape

                axis_map[file_axis.id] = None

                continue

            selector = slice(axis_slice[0], min(axis_slice[1], shape))

            logger.info('%r of %r is included in domain', selector, file_name)

            axis_slice[0] -= selector.start

            axis_slice[1] -= (selector.stop - selector.start) + selector.start

            if selector.stop == 0:
                logger.info('%r is not included in domain', file_name)

                axis_map[file_axis.id] = None
            else:
                logger.info('%r is included, mapping remaining axes', file_name)

                axis_map[file_axis.id] = selector

                exclude = [file_axis.id,]

                remaining = map_remaining_axes(x, var_name, domain, exclude)

                axis_map.update(remaining)

            file_axis_map[file_name] = axis_map

        attrs['axis_map'] = file_axis_map

    return attrs

@base.cwt_shared_task()
def map_axis_values(self, base_units, uri, var_name, axis, domain, user_id):
    try:
        user = models.User.objects.get(pk=user_id)
    except models.User.DoesNotExist:
        raise WPSError('User "{id}" does not exist', id=user_id)

    credentials.load_certificate(user)

    axis_slice = get_axis_slice(domain, axis)

    attrs = {
        'var_name': var_name,
        'axis': axis,
        'axis_slice': axis_slice[:],
    }

    with cdms2.open(uri) as infile:
        uri = get_uri(infile)

        axis = get_axis(infile, var_name, axis)

        axis_clone = axis.clone()

        axis_clone.toRelativeTime(base_units)

        try:
            interval = axis_clone.mapInterval(axis_slice)
        except TypeError:
            axis_map = { uri: None }
        else:
            axis_map = { 
                uri: {
                    axis_clone.id: slice(interval[0], interval[1]),
                }
            }

            exclude = [axis_clone.id,]

            remaining = map_remaining_axes(infile, var_name, domain, exclude)

            axis_map[uri].update(remaining)

    attrs['axis_map'] = axis_map

    return attrs

@base.cwt_shared_task()
def determine_base_units(self, uris, var_name, axis, user_id):
    try:
        user = models.User.objects.get(pk=user_id)
    except models.User.DoesNotExist:
        raise WPSError('User "{id}" does not exist', id=user_id)

    credentials.load_certificate(user)

    units = []

    for uri in uris:
        with cdms2.open(uri) as infile:
            file_axis = get_axis(infile, var_name, axis)

            units.append(file_axis.units)

    try:
        return sorted(units)[0]
    except IndexError:
        raise WPSError('Unable to determine base units for "{uris}"', uris=','.join(uris))
