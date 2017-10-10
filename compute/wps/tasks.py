#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import os
import json
import signal
import tempfile
from contextlib import closing
from datetime import datetime

import cdms2
import cwt
import django
import redis
import requests
import zmq
from celery import shared_task
from celery.signals import celeryd_init
from celery.utils.log import get_task_logger
from cwt.wps_lib import metadata
from cwt.wps_lib import operations
from openid.consumer import discover
from OpenSSL import crypto

from wps import models
from wps import settings
from wps import wps_xml
from wps.auth import oauth2
from wps.processes import process
from wps.processes import get_process
from wps.processes import CWTBaseTask

logger = get_task_logger('wps.tasks')

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_RESOURCE = 'urn:esg:security:oauth:endpoint:resource'

def openid_find_service_by_type(services, uri):
    for s in services:
        if uri in s.type_uris:
            return s

    return None

class WPSTaskError(Exception):
    pass

def create_job(server, status=None, result=None):
    """ Creates a Job entry. """
    if status is None:
        status = metadata.ProcessStarted()

    job = models.Job(server=server)

    job.save()

    job.status_set.create(status=str(status))
    
    return job

def create_status_location(host, job_id, port=None):
    """ Format status location. """
    loc = 'http://{0}'.format(host)

    if port is not None:
        loc = '{0}:{1}'.format(loc, port)

    loc = '{0}/wps/job/{1}'.format(loc, job_id)

    return loc

def default_server():
    """ Retreives the default server. """
    try:
        return models.Server.objects.get(host='default')
    except models.Server.DoesNotExist:
        raise WPSTaskError('Default server does not exist')

@shared_task(bind=True)
def edas_listen(self, address, response_port):
    context = zmq.Context.instance()

    socket = context.socket(zmq.SUB)

    socket.connect('tcp://{}:{}'.format(address, response_port))

    while True:
        data = socket.recv()

        logger.info(data)

@shared_task(bind=True, base=CWTBaseTask)
def check_auth(self, **kwargs):
    self.PUBLISH = process.RETRY | process.FAILURE

    self.set_user_creds(**kwargs)

    user_id = kwargs.get('user_id')

    try:
        user = models.User.objects.get(pk=user_id)
    except models.User.DoesNotExist:
        raise Exception('Could not find user')

    cert = crypto.load_certificate(crypto.FILETYPE_PEM, user.auth.cert)

    fmt = '%Y%m%d%H%M%SZ'

    before = datetime.strptime(cert.get_notBefore(), fmt)

    after = datetime.strptime(cert.get_notAfter(), fmt)

    now = datetime.now()

    if (now >= before and now <= after):
        logger.info('Certificate is still valid')

        return

    logger.info('Certificate has expired, renewing')

    if user.auth.type == 'myproxyclient':
        raise Exception('Please relog into MyProxyClient from your user account page.')

    url, services = discover.discoverYadis(user.auth.openid_url)

    auth_service = openid_find_service_by_type(services, URN_AUTHORIZE)

    cert_service = openid_find_service_by_type(services, URN_RESOURCE)

    extra = json.loads(user.auth.extra)

    try:
        token = extra['token']
    except KeyError:
        raise Exception('Missing OAuth2 token, try OAuth2 authentication again')

    cert, key, new_token = oauth2.get_certificate(token, auth_service.server_url, cert_service.server_url)

    logger.info('Recieved new token {}, updating certificate'.format(new_token))

    extra['token'] = new_token

    user.auth.extra = json.dumps(extra)

    user.auth.cert = ''.join([cert, key])

    user.auth.save()
