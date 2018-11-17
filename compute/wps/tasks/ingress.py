#! /usr/bin/env python

import contextlib
import datetime
import hashlib
import os
import psutil
from urlparse import urlparse

import cdms2
from cdms2 import MV2 as MV
from django.conf import settings
from celery.utils import log

from wps import helpers
from wps import metrics
from wps import models
from wps import WPSError
from wps.tasks import base
from wps.tasks import preprocess
from wps.utils import Timer

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
def ingress_cleanup(self, attrs, file_paths, job_id):
    for path in file_paths:
        try:
            os.remove(path)
        except:
            logger.warning('Failed to remove %r', path)

    return attrs

@base.cwt_shared_task()
def ingress_cache(self, attrs, uri, var_name, domain, chunk_axis_name, base_units, job_id):
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
    entry = preprocess.check_cache_entries(uri, var_name, domain, job_id)

    # Really should never hit this, if we're calling this task the file should
    # have been ingressed.
    if entry is not None:
        logger.info('%r of %r has been cached', var_name, uri)

        return attrs

    filter_ingress = [x for x in attrs.values() if isinstance(x, dict) and 'ingress' in x]

    filter_uri = [x for x in filter_ingress if x['uri'] == uri]

    filter_uri_sorted = sorted(filter_uri, key=lambda x: x['path'])

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

    entry = models.Cache(**kwargs)

    logger.info('Created cached entry for %r of %r with dimensions %r', var_name, uri, kwargs['dimensions'])

    chunk_axis = None
    chunk_list = []

    try:
        with self.open(entry.local_path, 'w') as outfile:
            for item in filter_uri_sorted:
                try:
                    with self.open(item['path']) as infile:
                        data = infile(var_name)

                        logger.info('Read chunk with shape %r', data.shape)

                        if chunk_axis is None:
                            chunk_axis_index = data.getAxisIndex(chunk_axis_name)

                            if chunk_axis_index == -1:
                                raise WPSError('Failed to find axis {}', chunk_axis_name)

                            chunk_axis = data.getAxis(chunk_axis_index)

                        if chunk_axis.isTime():
                            if base_units is not None:
                                data.getTime().toRelativeTime(str(base_units))

                            outfile.write(data, id=var_name)
                        else:
                            chunk_list.append(data)
                except cdms2.CDMSError as e:
                    raise base.AccessError(item['path'], e)
            
            if not chunk_axis.isTime():
                data = MV.concatenate(chunk_list, axis=chunk_axis_index)

                outfile.write(data, id=var_name)

            logger.info('Wrote cache file with shape %r', outfile[var_name].shape)
    except cdms2.CDMSError as e:
        logger.exception('Failed to write local file %r', entry.local_path)

        local_path = entry.local_path

        entry.delete()

        raise base.AccessError(local_path, e)

    entry.set_size()

    return attrs

@base.cwt_shared_task()
def ingress_uri(self, uri, var_name, group_maps, user_id, job_id):
    """ Ingress a portion of data.

    Args:
        key: A str containing a unique identifier for this request.
        uri: A str uri of the source file.
        var_name: A str variable name.
        group_maps: A dict mapping file paths to chunk slices.
        user_id: An int referencing the owner of the request.
        job_id: An int referencing the job this request belongs to.

    Return:
        A dict with the following format:

        key: Unique identifier.
        path: A string path to the output file.
        elapsed: A datetime.timedelta containing the elapsed time.
        size: A float denoting the size in MegaBytes.

        {
            "output_path": {
                "ingress": True,
                "uri": "https://aims3.llnl.gov/path/filename.nc",
                "path": "output_path",
                "elapsed": datetime.timedelta(0, 0, 3),
                "size": 3.28
            }
        }
    """
    self.load_credentials(user_id)

    job = self.load_job(job_id)

    parsed = urlparse(uri)

    host = 'local' if parsed.netloc == '' else parsed.netloc

    attrs = {}
    total_size = 0
    total_elapsed = datetime.timedelta()

    for output_path, domain in group_maps.iteritems():
        start = get_now()

        with self.open(uri) as infile:
            data = infile(var_name, **domain)

        shape = data.shape

        with self.open(output_path, 'w') as outfile:
            outfile.write(data, id=var_name)

        elapsed = get_now() - start

        total_elapsed += elapsed

        stat = os.stat(output_path)

        total_size += stat.st_size

        logger.info('Ingressed %r size %r in %r to %r', shape, stat.st_size,
                    elapsed.total_seconds(), output_path)

        attrs[output_path] = {
            'ingress': True,
            'uri': uri,
            'path': output_path,
        }

    metrics.INGRESS_BYTES.labels(host.lower()).inc(total_size)

    metrics.INGRESS_SECONDS.labels(host.lower()).inc(total_elapsed.total_seconds())

    size = total_size / 1000000.0

    return attrs
