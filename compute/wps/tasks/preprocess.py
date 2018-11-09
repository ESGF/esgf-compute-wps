#! /usr/bin/env python

import contextlib
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
from wps import models
from wps import WPSError
from wps.tasks import base

logger = get_task_logger('wps.tasks.preprocess')

@base.cwt_shared_task()
def wps_execute(self, attrs, variable, domain, operation, user_id, job_id):
    job = self.load_job(job_id)

    if not isinstance(attrs, list):
        attrs = [attrs]

    new_attrs = {}

    for item in attrs:
        new_attrs.update(item)

    new_attrs['user_id'] = user_id

    new_attrs['job_id'] = job_id

    new_attrs['preprocess'] = True

    # Collect inputs which are processes themselves
    inputs = [y.name for x in operation.values() for y in x.inputs 
              if isinstance(y, cwt.Process)]

    # Find the processes which do not belong as an input
    candidates = [x for x in operation.values() if x.name not in inputs]

    if len(candidates) > 1:
        raise WPSError('Invalid workflow there should only be a single root process')

    new_attrs['identifier'] = candidates[0].identifier
    
    new_attrs['root'] = candidates[0].name

    # Workflow if inputs includes any processes
    new_attrs['workflow'] = True if len(inputs) > 0 else False

    new_attrs['operation'] = operation

    new_attrs['domain'] = domain

    new_attrs['variable'] = variable

    data = helpers.encoder(new_attrs)

    try:
        response = requests.post(settings.WPS_EXECUTE_URL, data=data, verify=False)
    except requests.ConnectionError:
        raise WPSError('Error connecting to {}', settings.WPS_EXECUTE_URL)
    except requests.HTTPError as e:
        raise WPSError('HTTP Error {}', e)

    if not response.ok:
        raise WPSError('Failed to request execute: {} {}', response.reason, response.status_code)

    self.update(job, 'Preprocessing finished, submitting for execution for execution')

@base.cwt_shared_task()
def generate_chunks(self, attrs, operation, uri, process_axes, job_id):
    job = self.load_job(job_id)

    self.update(job, 'Generating chunks for {!r}', uri)

    try:
        mapped = attrs[uri]['cached']['mapped']
    except (TypeError, KeyError):
        mapped = attrs[uri]['mapped']

    if mapped is None:
        attrs[uri]['chunks'] = None

        return attrs

    # Determine the axis to generate chunks for, defaults to time axis
    if operation.identifier == 'CDAT.average' and len(process_axes) > 1:
        chunked_axis = None
    elif process_axes is None or 'time' not in process_axes:
        chunked_axis = 'time'
    else:
        process_axes = set(process_axes)

        mapped_axes = set(mapped.keys())

        candidates = mapped_axes - process_axes

        chunked_axis = list(candidates)[0]

    axis_size = dict((x, (y.stop-y.start)/y.step) for x, y in mapped.iteritems())

    chunk_axis_size = dict(x for x in axis_size.items() if chunked_axis is None
                           or x[0] != chunked_axis)

    logger.info('Worker memory size %r', settings.WORKER_MEMORY)

    est_total_size = reduce(lambda x, y: x*y, axis_size.values())*4

    logger.info('Estimated size in memory %r', est_total_size)

    est_chunk_size = reduce(lambda x, y: x*y, chunk_axis_size.values())*4

    logger.info('Estimated chunk size in memory %r', est_chunk_size)

    chunks_total = est_total_size/est_chunk_size

    logger.info('Chunks total %r', chunks_total)

    chunks_per_worker = round((settings.WORKER_MEMORY*0.50)/est_chunk_size)

    logger.info('Chunks per worker %r', chunks_per_worker)

    if chunks_per_worker == 0:
        raise WPSError('Chunk size of %r bytes is too large for workers memory try reducing the size by subsetting', 
                       est_chunk_size)

    partition_size = int(min(chunks_per_worker, chunks_total))

    chunks = []

    chunk_axis = mapped[chunked_axis]

    for begin in xrange(chunk_axis.start, chunk_axis.stop, partition_size):
        end = min(begin+partition_size, chunk_axis.stop)

        chunks.append(slice(begin, end, chunk_axis.step))

    attrs[uri]['chunks'] = {
        chunked_axis: chunks,
    }

    self.update(job, 'Generated {} chunks for {}', len(chunks), uri)

    return attrs

def check_cache_entries(uri, var_name, domain, job_id):
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

        if entry.is_superset(domain):
            logger.info('Found a valid cache entry for %r', uri)

            return entry

    logger.info('Found no valid cache entries for %r', uri)

    return None

@base.cwt_shared_task()
def check_cache(self, attrs, uri, var_name, job_id):
    job = self.load_job(job_id)

    mapped = attrs[uri]['mapped']

    if mapped is None:
        attrs[uri]['cached'] = None

        return attrs

    entry = check_cache_entries(uri, var_name, mapped, job_id)

    if entry is None:
        attrs[uri]['cached'] = None

        self.update(job, '{} from {} has not been cached', var_name, uri)
    else:
        dimensions = helpers.decoder(entry.dimensions)

        new_mapped = {}

        for key in mapped.keys():
            mapped_axis = mapped[key]

            orig_axis = dimensions[key]

            start = mapped_axis.start - orig_axis.start

            stop = mapped_axis.stop - orig_axis.start

            new_mapped[key] = slice(start, stop, mapped_axis.step)

        attrs[uri]['cached'] = {
            'path': entry.local_path,
            'mapped': new_mapped,
        }

        self.update(job, 'Found {} from {} in cache', var_name, uri)    

    return attrs

def map_domain_axis(axis, dimen, base_units):
    if dimen is None:
        selector = slice(0, len(axis), 1)

        logger.info('Selecting entire axis %r', selector)
    else:
        start = helpers.int_or_float(dimen.start)

        end = helpers.int_or_float(dimen.end)

        step = helpers.int_or_float(dimen.step)

        if dimen.crs == cwt.VALUES:
            if axis.isTime() and base_units is not None:
                axis = axis.clone()

                axis.toRelativeTime(base_units)

                logger.info('Rebased units to %r', base_units)

            try:
                mapped = axis.mapInterval((start, end))
            except TypeError:
                selector = None
            else:
                selector = slice(mapped[0], mapped[1], step)
        elif dimen.crs == cwt.INDICES:
            selector = slice(start, min(len(axis), end), step)
        else:
            raise WPSError('Unknown CRS value {}', dimen.crs)

        logger.info('Mapped %r:%r:%r to %r', start, end, step, selector)

    return selector

@base.cwt_shared_task()
def map_domain_time_indices(self, attrs, var_name, domain, user_id, job_id):
    """ Maps domain.

    Handles a special case when aggregte multiple files and the user defined
    time dimension is passed as indices. We cannot map this in parallel due
    to a dependency on the previously mapped file.
    """
    job = self.load_job(job_id)

    self.update(job, 'Mapping domain "{}" for aggregation', domain.name)

    self.load_credentials(user_id)

    sort = attrs['sort']

    base_units = attrs['base_units']

    urls = sorted(attrs['files'].items(), key=lambda x: x[1][sort])

    try:
        time = [x for x in domain.dimensions if x.name.lower() == 'time'][0]
    except IndexError:
        raise WPSError('Failed to find time dimension')

    mapped = {}

    for item in urls:
        url = item[0]

        mapped_domain = map_domain(attrs, url, var_name, domain, user_id, job_id)

        try:
            mapped_time = [x for x in mapped_domain[url]['mapped'].items() if x[0].lower() == 'time'][0]
        except IndexError:
            raise WPSError('Failed to find mapped time axis')

        diff = mapped_time[1].stop - mapped_time[1].start

        # Adjust the dimension by removing what we've consumed
        time.end -= diff + time.start

        time.start -= diff

        # Prevent the start from dipping into the negatives
        time.start = max(0, time.start)

        mapped.update(mapped_domain)

    self.update(job, 'Finished mapping domain')

    return mapped

@base.cwt_shared_task()
def map_domain(self, attrs, uri, var_name, domain, user_id, job_id):
    job = self.load_job(job_id)

    self.update(job, 'Mapping domain {}', domain)

    self.load_credentials(user_id)

    sort = attrs['sort']

    base_units = attrs['base_units']

    new_attrs = { 'sort': sort, 'base_units': base_units }

    file_attrs = attrs['files'][uri]

    with self.open(uri) as infile:
        variable = self.get_variable(infile, var_name)

        if domain is None:
            mapped = dict((x.id, map_domain_axis(x, None, base_units)) for x in variable.getAxisList())
        else:
            axes_indexes = dict((x, None) for x in variable.getAxisListIndex())

            mapped_dimen = [(x, variable.getAxisIndex(x.name)) for x in domain.dimensions]

            if any(True if x[1] == -1 else False for x in mapped_dimen):
                raise WPSError('Axes {} are missing from {}', ', '.join(x[0].name for x in mapped_dimen if x[1] == -1), uri)

            mapped = {}

            for item in mapped_dimen:
                axes_indexes.pop(item[1])

                axis = variable.getAxis(item[1])

                mapped[axis.id] = map_domain_axis(axis, item[0], base_units)

                if mapped[axis.id] == None:
                    mapped = None

                    break

            if mapped is not None:
                for index in axes_indexes.keys():
                    axis = variable.getAxis(index)

                    mapped[axis.id] = map_domain_axis(axis, None, base_units)

    self.update(job, 'Finished mapping domain')

    file_attrs['mapped'] = mapped

    new_attrs[uri] = file_attrs

    return new_attrs

@base.cwt_shared_task()
def determine_base_units(self, uris, var_name, user_id, job_id):
    job = self.load_job(job_id)

    self.load_credentials(user_id)

    files = {}

    for uri in uris:
        with self.open(uri) as infile:
            time = self.get_variable(infile, var_name).getTime()

            files[uri] = {
                'units': time.units,
                'first': time[0],
            }

    sort = None
    base_units = None

    check_units = reduce(lambda x, y: x if (x is not None and x['units'] ==
                                            y['units']) else None, files.values())

    if check_units is None:
        sort = 'units'

        units = [x['units'] for x in files.values()]

        base_units = reduce(lambda x, y: x if x <= y else y, units)

        self.update(job, 'Sorting inputs by time units, base at {}', base_units)    
    else:
        sort = 'first'

        self.update(job, 'Sorting inputs by time first values')

    return { 'files': files, 'sort': sort, 'base_units': base_units }
