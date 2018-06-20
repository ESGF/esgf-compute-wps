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

def get_variable(infile, var_name):
    return infile[var_name]

def get_axis_list(variable):
    return variable.getAxisList()

@base.cwt_shared_task()
def map_domain(self, attrs, uri, var_name, domain, user_id):
    load_credentials(user_id)

    base_units = attrs['base_units']

    return attrs

@base.cwt_shared_task()
def determine_base_units(self, uris, var_name, user_id):
    load_credentials(user_id)

    base_units = []

    for uri in uris:
        with cdms2.open(uri) as infile:
            base_units.append(get_variable(infile, var_name).getTime().units)

    try:
        return { 'base_units': sorted(base_units)[0] }
    except IndexError:
        raise WPSError('Unable to determine base units')
