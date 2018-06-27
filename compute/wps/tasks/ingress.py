#! /usr/bin/env python

import contextlib
import datetime
import hashlib
import os

import cdms2
from django.conf import settings
from celery.utils import log

from wps import helpers
from wps import models
from wps import WPSError
from wps.tasks import base
from wps.tasks import preprocess

__ALL__ = [
    'preprocess',
    'ingress',
]

logger = log.get_task_logger('wps.tasks.ingress')

def get_now():
    return datetime.datetime.now()

def read_data(infile, var_name, domain):
    time = None

    if 'time' in domain:
        time = domain.pop('time')

    if time is None:
        data = infile(var_name, **domain)
    else:
        data = infile(var_name, time=time, **domain)

    return data

@base.cwt_shared_task()
def ingress_cleanup(self, attrs):
    uris = [y['path'] for x in attrs for y in attrs[x]['ingress']]

    for uri in uris:
        try:
            os.remove(uri)
        except OSError:
            logger.exception('Failed to remove %r', uri)

            continue

    return attrs

@base.cwt_shared_task()
def ingress_cache(self, attrs, uri, var_name, domain, base_units):
    entry = preprocess.check_cache_entries(uri, var_name, domain)

    if entry is not None:
        logger.info('%r of %r has been cached', var_name, uri)

        return attrs

    uri_meta = attrs[uri]

    uid = '{}:{}'.format(uri, var_name)

    dimensions = {
        'var_name': var_name,
    }

    dimensions.update(domain)
    
    kwargs = {
        'uid': hashlib.sha256(uid).hexdigest(),
        'url': uri,
        'dimensions': helpers.encoder(dimensions),
    }

    entry = models.Cache.objects.create(**kwargs)

    logger.info('Created cached entry for %r of %r with dimensions %r', var_name, uri, kwargs['dimensions'])

    try:
        with cdms2.open(entry.local_path, 'w') as outfile:
            for ingress_meta in uri_meta['ingress']:
                try:
                    with cdms2.open(ingress_meta['path']) as infile:
                        data = infile(var_name)

                        logger.info('Read chunk with shape %r', data.shape)

                        data.getTime().toRelativeTime(str(base_units))

                        outfile.write(data, id=var_name)
                except cdms2.CDMSError as e:
                    raise base.AccessError(ingress_uri, e)
            
            logger.info('Wrote cache file with shape %r', outfile[var_name].shape)
    except cdms2.CDMSError as e:
        logger.exception('Failed to write local file %r', entry.local_path)

        local_path = entry.local_path

        entry.delete()

        raise base.AccessError(local_path, e)
    except base.AccessError:
        logger.exception('Failed to read ingress file %r', ingress_uri)

        entry.delete()

        raise
    entry.set_size()

    return attrs

@base.cwt_shared_task()
def ingress_uri(self, uri, var_name, domain, output_path, base_units, user_id, job_id):
    start = get_now()

    logger.info('Domain %r', domain)

    try:
        with cdms2.open(uri) as infile:
            data = read_data(infile, var_name, domain)
    except cdms2.CDMSError:
        raise WPSError('Failed to open "{uri}"', uri=uri)

    logger.info('Read data shape %r', data.shape)

    try:
        with cdms2.open(output_path, 'w') as outfile:
            outfile.write(data, id=var_name)
    except cdms2.CDMSError:
        raise WPSError('Failed to open "{uri}"', uri=output_path)

    elapsed = get_now() - start

    stat = os.stat(output_path)

    attrs = {
        uri: {
            'base_units': base_units,
            'ingress': {
                'path': output_path,
                'elapsed': elapsed,
                'size': stat.st_size / 1000000.0,
            },
        },
    }

    return attrs
