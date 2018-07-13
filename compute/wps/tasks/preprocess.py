#! /usr/bin/env python

import contextlib
import hashlib
import json
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
def analyze_wps_request(self, attrs_list, variable, domain, operation, user_id, job_id=None):
    job = self.load_job(job_id)

    self.update(job, 'Preparing to execute')

    attrs = {}

    if not isinstance(attrs_list, list):
        attrs_list = [attrs_list,]

    # Combine the list of inputs
    for item in attrs_list:
        for key, value in item.iteritems():
            if key not in attrs:
                attrs[key] = value
            else:
                try:
                    attrs[key].update(value)
                except AttributeError:
                    continue

    attrs['user_id'] = user_id

    attrs['job_id'] = job_id

    attrs['preprocess'] = True

    # Collect inputs which are processes themselves
    inputs = [y.name
              for x in operation.values()
              for y in x.inputs
              if isinstance(y, cwt.Process)]

    # Find the processes which do not belong as an input
    candidates = [x for x in operation.values() if x.name not in inputs]

    if len(candidates) > 1:
        raise WPSError('Invalid workflow there should only be a single root process')

    attrs['root'] = candidates[0].name

    # Workflow if inputs includes any processes
    attrs['workflow'] = True if len(inputs) > 0 else False

    attrs['operation'] = operation

    attrs['domain'] = domain

    attrs['variable'] = variable

    estimated_size = 0.0

    # estimate total size
    for item in attrs['mapped'].values():
        size = reduce(lambda x, y: x * y, [x.stop-x.start for x in item.values()])

        # Convert from bytes to megabytes, the 0.4 factor is just an estimation,
        # to compensate for compression
        estimated_size += size * 0.4 / 1000000.0

    attrs['estimated_size'] = estimated_size

    return attrs

@base.cwt_shared_task()
def request_execute(self, attrs, job_id=None):
    data = {
        'data': helpers.encoder(attrs)
    }

    response = requests.post(settings.WPS_EXECUTE_URL, data=data, verify=False)

    if not response.ok:
        raise WPSError('Failed to execute status code {code}', code=response.status_code)

@base.cwt_shared_task()
def generate_chunks(self, attrs, uri, process_axes, job_id=None):
    job = self.load_job(job_id)

    try:
        mapped = attrs['cached'][uri]['mapped']
    except (KeyError, ValueError, TypeError):
        logger.info('%r has not been cached', uri)

        mapped = None

    if mapped is None:
        try:
            mapped = attrs['mapped'][uri]
        except KeyError:
            raise WPSError('{uri} has not been mapped', uri=uri)

    TIME = ['time', 't', 'z']

    # If process_axis is None we'll default to the time axis
    if process_axes is None:
        axis = [x for x in mapped.keys() if x.lower() in TIME][0]
    else:
        # Determine which axes we're not processing
        axis = [x for x in mapped.keys() if x.lower() not in process_axes]

        # Check if time is one of the not processing axes
        over_time = any([True for x in axis if x in TIME])

        if over_time:
            # Set the axis to time
            axis = [x for x in mapped.keys() if x.lower() in TIME][0]
        else:
            # Try to choose the first non-time axis
            try:
                axis = axis[0]
            except IndexError:
                # Covers a case where we're processing over all axes and we'll
                # choose the time as default
                axis = [x for x in mapped.keys() if x.lower() in TIME][0]

    logger.info('Generating chunks over %r', axis)

    # Covers an unmapped axis
    try:
        chunked_axis = mapped[axis]
    except TypeError:
        attrs['chunks'] = {
            uri: None
        }

        return attrs
    except KeyError:
        raise WPSError('Missing "{axis}" axis in the axis mapping', axis=axis)

    chunks = []

    for begin in xrange(chunked_axis.start, chunked_axis.stop, settings.WPS_PARTITION_SIZE):
        end = min(begin+settings.WPS_PARTITION_SIZE, chunked_axis.stop)

        chunks.append(slice(begin, end))

    self.update(job, 'Generated chunks for {} over {}', uri, axis)

    attrs['chunks'] = {
        uri: {
            axis: chunks
        }
    }

    return attrs

def check_cache_entries(uri, var_name, domain, job_id=None):
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
def check_cache(self, attrs, uri, job_id=None):
    job = self.load_job(job_id)

    var_name = attrs['var_name']

    mapped = attrs['mapped'][uri]

    entry = check_cache_entries(uri, var_name, mapped)

    if entry is None:
        attrs['cached'] = {
            uri: None
        }

        self.update(job, '{} from {} has not been cached', var_name, uri)
    else:
        dimensions = helpers.decoder(entry.dimensions)

        new_mapped = {}

        for key in mapped.keys():
            mapped_axis = mapped[key]

            orig_axis = dimensions[key]

            start = mapped_axis.start - orig_axis.start

            stop = mapped_axis.stop - orig_axis.start

            new_mapped[key] = slice(start, stop)

        stat = os.stat(entry.local_path)

        attrs['cached'] = {
            uri: {
                'path': entry.local_path,
                'mapped': new_mapped,
                'size': stat.st_size / 1000000.0,
            }
        }

        self.update(job, 'Found {} from {} in cache', var_name, uri)    

    return attrs

@base.cwt_shared_task()
def map_domain_aggregate(self, attrs, uris, var_name, domain, user_id, job_id=None):
    load_credentials(user_id)

    job = self.load_job(job_id)

    base_units = attrs['base_units']

    attrs['var_name'] = var_name

    attrs['mapped'] = {}

    time_dimen = None

    with contextlib.nested(*[cdms2.open(x) for x in uris]) as infiles:
        for infile in infiles:
            uri = get_uri(infile)

            self.update(job, 'Mapping domain for {} from {}', var_name, uri)

            mapped = {}

            variable = get_variable(infile, var_name)

            for axis in get_axis_list(variable):
                try:
                    dimen = domain.get_dimension(axis.id)
                except AttributeError:
                    dimen = None

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

            attrs['mapped'][uri] = mapped

    return attrs

@base.cwt_shared_task()
def map_domain(self, attrs, uri, var_name, domain, user_id, job_id=None):
    load_credentials(user_id)

    job = self.load_job(job_id)

    base_units = attrs['base_units']

    attrs['var_name'] = var_name

    mapped = {}

    with cdms2.open(uri) as infile:
        self.update(job, 'Mapping domain for {} from {}', var_name, uri)

        variable = get_variable(infile, var_name)

        for axis in get_axis_list(variable):
            try:
                dimen = domain.get_dimension(axis.id)
            except AttributeError:
                dimen = None

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

        attrs['mapped'] = { uri: mapped }

    return attrs

@base.cwt_shared_task()
def determine_base_units(self, uris, var_name, user_id, job_id=None):
    job = self.load_job(job_id)

    job.started()

    load_credentials(user_id)

    attrs = {
        'units': {},
    }

    base_units_list = []

    for uri in uris:
        with cdms2.open(uri) as infile:
            base_units = get_variable(infile, var_name).getTime().units

        attrs['units'][uri] = base_units

        base_units_list.append(base_units)

    self.update(job, 'Collected base units {}', base_units_list)

    try:
        attrs['base_units'] = sorted(base_units_list)[0]
    except IndexError:
        raise WPSError('Unable to determine base units')

    return attrs
