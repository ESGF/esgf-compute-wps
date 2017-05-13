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
from OpenSSL import crypto

from wps import models
from wps import settings
from wps import wps_xml
from wps.auth import oauth2
from wps.auth import openid
from wps.processes import get_process
from wps.processes import CWTBaseTask
from wps.processes import handle_output

logger = get_task_logger(__name__)

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_RESOURCE = 'urn:esg:security:oauth:endpoint:resource'

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

def create_socket(host, port, socket_type):
    """ Create a ZMQ socket. """
    context = zmq.Context.instance()

    socket = context.socket(socket_type)

    socket.connect('tcp://{0}:{1}'.format(host, port))

    return socket

def default_server():
    """ Retreives the default server. """
    try:
        return models.Server.objects.get(host='default')
    except models.Server.DoesNotExist:
        raise WPSTaskError('Default server does not exist')

@celeryd_init.connect
def monitor_handler(**kwargs):
    """ Monitor CDAS2 queue.

    Create a monitor for each CDAS2 instance.
    """
    logger.info('celeryd_init, starting monitors')

    instances = models.Instance.objects.all()

    try:
        for i in instances:
            monitor_cdas.delay(i.id)
    except django.db.utils.ProgrammingError:
        logger.info('Database does not appear to be setup, not starting monitors')

@shared_task
def handle_response(data):
    """ Handle CDAS2 responses.

    Convert the CDAS2 response to the appropriate WPS operation response.
    """
    job_id, _, response = data.split('!')

    logger.info('Handling CDAS2 response for job %s', job_id)

    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        # Really should never hist this point
        logger.exception('Job %s does not exist', job_id)

        return

    error = wps_xml.check_cdas2_error(response)

    if error is not None:
        job.failed(error)

        return

    cap = 'capabilities' in data

    desc = 'processDescription' in data

    if cap or desc:
        try:
            if cap:
                local_procs = models.Process.objects.filter(backend='local')

                result = wps_xml.capabilities_response(response, local_procs)
            else:
                result = wps_xml.describe_process_response_from_cdas2(response)
        except Exception as e:
            logger.exception('Failed to convert CDAS2 response: %s', e.message)

            job.failed(e.message)

            return

        if cap:
            job.server.capabilities = result.xml()

            job.server.save()

            identifiers = [x.identifier for x in result.process_offerings]

            logger.info('Queueing DescribeProcess for following proccesses: %s', identifiers)

            describe.delay(job.server.id, identifiers)
        else:
            result = wps_xml.describe_process_response_from_cdas2(response)
            
            server = default_server()

            process = models.Process(
                    identifier=result.process_description[0].identifier,
                    backend='CDAS2',
                    description=result.xml())
            
            process.save()

            server.processes.add(process)

        job.succeeded()
    else:
        job.update_report_cdas(response)

@shared_task
def monitor_cdas(instance_id):
    """ Monitor CDAS2 queue.

    Start a handler task for each CDAS2 message that pops off the queue.
    """
    try:
        instance = models.Instance.objects.get(pk=instance_id)
    except models.Instance.DoesNotExist:
        logger.info('Instance id "%s" does not exist', instance_id)

        return

    logger.info('Monitoring CDAS instance at %s:%s', instance.host, instance.response)

    with closing(create_socket(instance.host, instance.response, zmq.PULL)) as response:
        while True:
            data = response.recv()

            handle_response.delay(data)

@shared_task
def capabilities(server_id):
    """ Handles GetCapabilities request. """
    try:
        server = models.Server.objects.get(pk=server_id)
    except models.Server.DoesNotExist:
        logger.info('Server id "%s" does not exist', server_id)

        return

    logger.info('Gathering "%s" capabilities', server.host)

    instances = models.Instance.objects.all()

    if len(instances) > 0:
        logger.info('Querying CDAS2 instance capabilities')

        instance = instances[0]

        job = create_job(server)

        with closing(create_socket(instance.host, instance.request, zmq.PUSH)) as request:
            request.send(str('{0}!getCapabilities!WPS'.format(job.id)))
    else:
        logger.info('Server has not CDAS2 instances')

@shared_task
def describe(server_id, identifiers):
    """ Handles a DescribeProcess request. """
    try:
        # TODO might want a better way of choosing
        instance = models.Instance.objects.all()
    except models.Instance.DoesNotExist:
        logger.info('Instance id "%s" does not exist', instance_id)

        return

    try:
        server = models.Server.objects.get(pk=server_id)
    except models.Instance.DoesNotExist:
        logger.info('Default server does not exist yet')

        return

    if len(instance) == 0:
        logger.info('No CDAS2 instance to run describe process for %s', identifier)

        return

    with closing(create_socket(instance[0].host, instance[0].request, zmq.PUSH)) as request:
        for identifier in identifiers:
            job = create_job(server)

            request.send(str('{0}!describeProcess!{1}'.format(job.id, identifier)))

@shared_task(bind=True, base=CWTBaseTask)
def check_auth(self, **kwargs):
    self.initialize(**kwargs)

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

    if user.auth.backend == 'mpc':
        raise Exception('Please relog into MyProxyClient from your user account page.')
    
    oid = openid.OpenID.parse(user.auth.openid)

    access = oid.find(URN_ACCESS)

    resource = oid.find(URN_RESOURCE)

    cert, key, new_token = oauth2.get_certificate(user.auth.token, access.uri, resource.uri)

    logger.info('Recieved new token {}, updating certificate'.format(new_token))

    user.auth.token = new_token

    user.auth.cert = ''.join([cert, key])

    user.auth.save()
