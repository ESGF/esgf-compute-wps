#! /usr/bin/env python

import contextlib
import hashlib
import json

import cwt
import cdms2
import requests
from celery.utils.log import get_task_logger
from django.conf import settings

from wps import helpers
from wps import models
from wps import WPSError
from wps.tasks import base
from wps.tasks import credentials

logger = get_task_logger('wps.tasks.preprocess')

def load_credentials(user_id):
    try:
        user = models.User.objects.get(pk=user_id)
    except models.User.DoesNotExist:
        raise WPSError('User "{id}" does not exist', id=user_id)

    credentials.load_certificate(user)

def get_uri(infile):
    return infile.uri

def get_variable(infile, var_name):
    return infile[var_name]

def get_axis_list(variable):
    return variable.getAxisList()

@base.cwt_shared_task()
def collect_and_execute(self, attrs):
    data = None

    if not isinstance(attrs, list):
        attrs = [attrs,]

    for attr in attrs:
        if data is None:
            data = attr
        else:
            data.update(attr)

    response = requests.post(settings.WPS_EXECUTE_URL, data=data, verify=False)

    if not response.ok:
        raise WPSError('Failed to execute status code {code}', code=response.status_code)

@base.cwt_shared_task()
def generate_chunks(self, attrs, uri, axis):
    logger.info('Generating chunks for %r over %r', uri, axis)

    file_attrs = attrs[uri]

    try:
        mapped = file_attrs['mapped']
    except KeyError:
        raise WPSError('{uri} has not been mapped', uri=uri)

    try:
        chunked_axis = mapped[axis]
    except TypeError:
        attrs[uri]['chunks'] = None

        return attrs
    except KeyError:
        raise WPSError('Missing %r axis in the axis mapping', axis)

    chunks = []

    for begin in xrange(chunked_axis.start, chunked_axis.stop, settings.WPS_PARTITION_SIZE):
        end = min(begin+settings.WPS_PARTITION_SIZE, chunked_axis.stop)

        chunks.append(slice(begin, end))

    logger.info('Split %r into %r chunks', uri, len(chunks))

    attrs[uri]['chunks'] = {
        axis: chunks
    }

    return attrs

@base.cwt_shared_task()
def check_cache(self, attrs, uri):
    var_name = attrs['var_name']

    file_attrs = attrs[uri]

    uid = '{}:{}'.format(uri, var_name)

    uid_hash = hashlib.sha256(uid).hexdigest()

    cache_entries = models.Cache.objects.filter(uid=uid_hash)

    logger.info('Found %r cache entries for %r', len(cache_entries), uid_hash)

    for x in xrange(cache_entries.count()):
        entry = cache_entries[x]

        if not entry.valid:
            logger.info('Entry for %r is invalid, removing', entry.uid)

            entry.delete()
            
            continue

        if entry.is_superset(file_attrs['mapped']):
            logger.info('Found a valid cache entry for %r', uri)

            file_attrs['cached'] = entry.url

            break

    if 'cached' not in file_attrs:
        logger.info('Found no valid cache entries for %r', uri)

        file_attrs['cached'] = None

    return attrs

@base.cwt_shared_task()
def map_domain_aggregate(self, attrs, uris, var_name, domain, user_id):
    load_credentials(user_id)

    base_units = attrs['base_units']

    attrs['var_name'] = var_name

    time_dimen = None

    with contextlib.nested(*[cdms2.open(x) for x in uris]) as infiles:
        for infile in infiles:
            uri = get_uri(infile)

            logger.info('Mapping %r from file %r', var_name, uri)

            mapped = {}

            variable = get_variable(infile, var_name)

            for axis in get_axis_list(variable):
                dimen = domain.get_dimension(axis.id)

                if dimen is None:
                    logger.info('Axis %r is not included in domain', axis.id)

                    shape = axis.shape[0]

                    selector = slice(0, shape)
                else:
                    if dimen.crs == cwt.VALUES:
                        start = helpers.int_or_float(dimen.start)

                        end = helpers.int_or_float(dimen.end)

                        selector = (start, end)

                        logger.info('Dimension %r converted to %r', axis.id, selector)
                    elif dimen.crs == cwt.INDICES:
                        if axis.isTime():
                            if time_dimen is None:
                                time_dimen = [int(dimen.start), int(dimen.end)]

                            if reduce(lambda x, y: x - y, time_dimen) == 0:
                                logger.info('Domain consumed skipping %r', uri)

                                mapped = None

                                break

                            shape = axis.shape[0]

                            selector = slice(time_dimen[0], min(time_dimen[1], shape))                            

                            time_dimen[0] -= selector.start

                            time_dimen[1] -= (selector.stop - selector.start) + selector.start
                        else:
                            start = int(dimen.start)

                            end = int(dimen.end)

                            selector = slice(start, end)

                        logger.info('Dimension %r converted to %r', axis.id, selector)
                    else:
                        raise WPSError('Unable to handle crs "{name}" for axis "{id}"', name=dimen.crs.name, id=axis.id)

                mapped[axis.id] = selector

            attrs[uri] = { 'mapped': mapped }

    return attrs

@base.cwt_shared_task()
def map_domain(self, attrs, uri, var_name, domain, user_id):
    load_credentials(user_id)

    base_units = attrs['base_units']

    attrs['var_name'] = var_name

    mapped = {}

    with cdms2.open(uri) as infile:
        logger.info('Mapping domain for %r', uri)

        variable = get_variable(infile, var_name)

        for axis in get_axis_list(variable):
            dimen = domain.get_dimension(axis.id)

            if dimen is None:
                shape = axis.shape[0]

                selector = slice(0, shape)

                logger.info('Dimension %r missing setting to %r', axis.id, selector)
            else:
                if dimen.crs == cwt.VALUES:
                    start = helpers.int_or_float(dimen.start)

                    end = helpers.int_or_float(dimen.end)

                    if axis.isTime():
                        logger.info('Setting time axis base units to %r', base_units)

                        axis = axis.clone()

                        axis.toRelativeTime(base_units)

                    try:
                        interval = axis.mapInterval((start, end))
                    except TypeError:
                        logger.info('Dimension %r could not be mapped, skipping', axis.id)

                        mapped = None

                        break
                    else:
                        selector = slice(interval[0], interval[1])

                    logger.info('Dimension %r mapped to %r', axis.id, selector) 
                elif dimen.crs == cwt.INDICES:
                    start = int(dimen.start)

                    end = int(dimen.end)

                    selector = (start, end)

                    logger.info('Dimension %r converted to %r', axis.id, selector)
                else:
                    raise WPSError('Unable to handle crs "{name}" for axis "{id}"', name=dimen.crs.name, id=axis.id)

            mapped[axis.id] = selector

        attrs[uri] = { 'mapped': mapped }

    return attrs

@base.cwt_shared_task()
def determine_base_units(self, uris, var_name, user_id):
    load_credentials(user_id)

    attrs = {}
    base_units = []

    for uri in uris:
        attrs[uri] = {}

        with cdms2.open(uri) as infile:
            base_units.append(get_variable(infile, var_name).getTime().units)

    logger.info('Collected base units %r', base_units)

    try:
        attrs['base_units'] = sorted(base_units)[0]
    except IndexError:
        raise WPSError('Unable to determine base units')

    return attrs
