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
def ingress_cleanup(self, attrs, job_id=None):
    """ Cleanup the ingressed files.

    Args:
        attrs: A list of dicts from previous tasks.
        job_id: An int referencing the job.

    Returns:
        A dict composed of the dicts in attrs argument.
    """
    filter_paths = [x['ingress']['path'] for x in attrs]

    for path in filter_paths:
        try:
            os.remove(path)
        except OSError:
            logger.exception('Failed to remove %r', path)

            continue

        logger.info('Removed ingress file %r', path)

    return attrs

@base.cwt_shared_task()
def ingress_cache(self, attrs, uri, var_name, domain, base_units, job_id=None):
    """ Cached ingress items.

    Args:
        attrs: A list of dicts from previous tasks.
        uri: A str uri for the source being cached.
        var_name: A str variable name.
        domain: A dict referencing the portion of the source file compose by the
            group of ingressed files.
        base_units: A str with the base units.
        job_id: An int referencing the job.

    Returns: 
        A dict composed of the dicts from attrs.
    """
    entry = preprocess.check_cache_entries(uri, var_name, domain)

    if entry is not None:
        logger.info('%r of %r has been cached', var_name, uri)

        return attrs

    filter_ingress = [x for x in attrs.values() if 'ingress' in x]

    filter_uri = [x for x in filter_ingress if x['ingress']['uri'] == uri]

    filter_uri_sorted = sorted(filter_uri, key=lambda x: x['ingress']['path'])

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
            for item in filter_uri_sorted:
                try:
                    with cdms2.open(item['ingress']['path']) as infile:
                        data = infile(var_name)

                        logger.info('Read chunk with shape %r', data.shape)

                        data.getTime().toRelativeTime(str(base_units))

                        outfile.write(data, id=var_name)
                except cdms2.CDMSError as e:
                    raise base.AccessError(item_meta['ingress']['path'], e)
            
            logger.info('Wrote cache file with shape %r', outfile[var_name].shape)
    except cdms2.CDMSError as e:
        logger.exception('Failed to write local file %r', entry.local_path)

        local_path = entry.local_path

        entry.delete()

        raise base.AccessError(local_path, e)

    entry.set_size()

    return dict(y for x in filter_uri_sorted for y in x.items())

@base.cwt_shared_task()
def ingress_uri(self, key, uri, var_name, domain, output_path, user_id, job_id=None):
    """ Ingress a portion of data.

    Args:
        key: A str containing a unique identifier for this request.
        uri: A str uri of the source file.
        var_name: A str variable name.
        domain: A dict containing the portion of the file.
        output_path: A str path for the output to be written.
        user_id: An int referencing the owner of the request.
        job_id: An int referencing the job this request belongs to.

    Return:
        A dict with the following format:

        key: Unique identifier.
        path: A string path to the output file.
        elapsed: A datetime.timedelta containing the elapsed time.
        size: A float denoting the size in MegaBytes.

        {
            "key": {
                "ingress": {
                    "uri": "https://aims3.llnl.gov/path/filename.nc",
                    "path": "file:///path/filename.nc",
                    "elapsed": datetime.timedelta(0, 0, 3),
                    "size": 3.28
                }
            }
        }
    """
    preprocess.load_credentials(user_id)

    job = self.load_job(job_id)

    start = get_now()

    logger.info('Domain %r', domain)

    try:
        with cdms2.open(uri) as infile:
            data = read_data(infile, var_name, domain)
    except cdms2.CDMSError:
        raise WPSError('Failed to open "{uri}"', uri=uri)

    shape = data.shape

    logger.info('Read data shape %r', shape)

    try:
        with cdms2.open(output_path, 'w') as outfile:
            outfile.write(data, id=var_name)
    except cdms2.CDMSError:
        raise WPSError('Failed to open "{uri}"', uri=output_path)

    elapsed = get_now() - start

    stat = os.stat(output_path)

    self.update(job, 'Ingressed chunk {}', shape)

    attrs = {
        key: {
            'ingress': {
                'uri': uri,
                'path': output_path,
                'elapsed': elapsed,
                'size': stat.st_size / 1000000.0,
            }
        }
    }

    return attrs
