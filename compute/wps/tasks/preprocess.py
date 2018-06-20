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
def map_domain_aggregate(self, attrs, uris, var_name, domain, user_id):
    load_credentials(user_id)

    base_units = attrs['base_units']

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

    base_units = []

    for uri in uris:
        with cdms2.open(uri) as infile:
            base_units.append(get_variable(infile, var_name).getTime().units)

    logger.info('Collected base units %r', base_units)

    try:
        return { 'base_units': sorted(base_units)[0] }
    except IndexError:
        raise WPSError('Unable to determine base units')
