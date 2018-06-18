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

def get_uri(infile):
    return infile.uri

def get_axis(infile, var_name, axis):
    variable = infile[var_name]

    axis_index = variable.getAxisIndex(axis)

    if axis_index == -1:
        raise WPSError('Axis "{id}" was not found in "{url}"', id=axis, url=infile.id)

    return variable.getAxis(axis_index)

def get_axis_list(infile, var_name, exclude=None):
    if exclude is None:
        exclude = []

    return [x for x in infile[var_name].getAxisList() if x.id not in exclude]

@base.cwt_shared_task()
def check_cache(self, attrs):
    if attrs.get('axis_map') is None:
        attrs['cached'] = None

        return attrs

    uid = '{}:{}'.format(attrs['uri'], attrs['var_name'])

    uid_hash = hashlib.sha256(uid).hexdigest()

    cache_entries = models.Cache.objects.filter(uid=uid_hash)

    domain = {}

    for name, value in attrs['axis_map'].iteritems():
        if name in ('time', 't', 'z'):
            domain['temporal'] = value
        else:
            if 'spatial' in domain:
                domain['spatial'][name] = value
            else:
                domain['spatial'] = {
                    name: value
                }

    for x in xrange(cache_entries.count()):
        entry = cache_entries[x]

        if not entry.valid:
            entry.delete()
            
            continue

        if entry.is_superset(domain):
            attrs['cached'] = x.url

            break

    if 'cached' not in attrs:
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

    axis_dimension = domain.get_dimension(axis)

    axis_slice = [
        helpers.int_or_float(axis_dimension.start), 
        helpers.int_or_float(axis_dimension.end),
    ]

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

                for axis in get_axis_list(x, var_name, [axis_dimension.name,]):
                    dimension = domain.get_dimension(axis.id)

                    if dimension is None:
                        continue

                    if dimension.crs == cwt.VALUES:
                        try:
                            axis_interval = axis.mapInterval((dimension.start, dimension.end))
                        except TypeError:
                            raise WPSError('Failed to map axis "{id}"', id=dimension.name)

                        axis_map[axis.id] = slice(axis_interval[0], axis_interval[1])
                    elif dimension.crs == cwt.INDICES:
                        aixs_map[axis.id] = slice(dimension.start, dimension.end)
                    else:
                        raise WPSError('Unable to handle crs %r', dimension.crs)

                    logger.info('Mapped %r to %r', axis.id, axis_map[axis.id])

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

    axis_dimension = domain.get_dimension(axis)

    axis_slice = [
        helpers.int_or_float(axis_dimension.start), 
        helpers.int_or_float(axis_dimension.end),
    ]

    attrs = {
        'uri': uri,
        'var_name': var_name,
        'axis': axis,
        'axis_slice': axis_slice[:],
    }

    with cdms2.open(uri) as infile:
        axis = get_axis(infile, var_name, axis)

        axis_clone = axis.clone()

        axis_clone.toRelativeTime(base_units)

        try:
            axis_interval = axis_clone.mapInterval(axis_slice)
        except TypeError:
            axis_map = None
        else:
            axis_map = {}

            axis_map[axis_clone.id] = slice(axis_interval[0], axis_interval[1])

            for dim in domain.dimensions:
                axis = get_axis(infile, var_name, dim.name)

                if axis.id in axis_map:
                    continue

                if dim.crs == cwt.VALUES:
                    try:
                        axis_interval = axis.mapInterval((dim.start, dim.end))
                    except TypeError:
                        raise WPSError('Failed to map axis "{id}"', id=dim.name)

                    axis_map[axis.id] = slice(axis_interval[0], axis_interval[1])
                elif dim.crs == cwt.INDICES:
                    axis_map[axis.id] = slice(dim.start, dim.end)

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
