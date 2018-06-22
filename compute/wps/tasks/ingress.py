#! /usr/bin/env python

import contextlib

import cdms2
from django.conf import settings
from celery.utils import log

from wps import WPSError
from wps.tasks import base

__ALL__ = [
    'preprocess',
    'ingress',
]

logger = log.get_task_logger('wps.tasks.ingress')

def read_data(infile, var_name, domain):
    time = domain.pop('time')

    if time is None:
        data = infile(var_name, **domain)
    else:
        data = infile(var_name, time=time, **domain)

    return data

@base.cwt_shared_task()
def ingress_uri(self, uri, var_name, domain, output_path, user_id, job_id):
    try:
        with cdms2.open(uri) as infile:
            data = read_data(infile, var_name, domain)
    except cdms2.CDMSError:
        raise WPSError('Failed to open "{uri}"', uri=uri)

    try:
        with cdms2.open(uri) as outfile:
            outfile.write(data, id=var_name)
    except cdms2.CDMSError:
        raise WPSError('Failed to open "{uri}"', uri=output_path)

    return output_path
