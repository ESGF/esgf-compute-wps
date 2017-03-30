#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import os
from contextlib import closing

import django
import redis
import zmq
from celery import shared_task
from celery.signals import celeryd_init
from celery.utils.log import get_task_logger
from cwt.wps_lib import metadata
from cwt.wps_lib import operations

from wps import models
from wps import wps_xml

logger = get_task_logger(__name__)

class WPSTaskError(Exception):
    pass

def create_job(server, status=None, result=None):
    """ Creates a Job entry. """
    if status is None:
        status = metadata.ProcessStarted()

    job = models.Job(server=server)

    job.save()

    job.status_set.create(status=wps_xml.status_to_int(status))
    
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
        latest = job.status_set.all().latest('created_date')

        message = wps_xml.update_execute_response_exception(latest.result, error)

        job.status_set.create(status=wps_xml.status_to_int(metadata.ProcessFailed()),
                result=message.xml())

        return

    cap = 'capabilities' in data

    desc = 'processDescription' in data

    if cap or desc:
        try:
            result = wps_xml.convert_cdas2_response(response)
        except Exception as e:
            logger.exception('Failed to convert CDAS2 response: %s', e.message)

            exc_report = metadata.ExceptionReport(wps_xml.VERSION)

            exc_report.add_exception(metadata.NoApplicableCode, e.message)

            latest_response = job.status_set.all().latest('created_date')

            result = wps_xml.update_execute_response_exception(latest_response.result, exc_report)

            job.status_set.create(status=wps_xml.status_to_int(metadata.ProcessFailed()), result=result.xml())

            return

        if cap:
            job.server.capabilities = result.xml()

            job.server.save()

            identifiers = [x.identifier for x in result.process_offerings]

            describe.delay(job.server.id, identifiers)
        else:
            result = wps_xml.create_describe_process_response(response)
            
            server = default_server()

            process = models.Process(
                    identifier=result.process_description[0].identifier,
                    backend='CDAS2',
                    description=result.xml())
            
            process.save()

            server.processes.add(process)
    else:
        latest_response = job.status_set.all().latest('created_date')

        result = wps_xml.update_execute_response(latest_response.result, response)

    job.status_set.create(status=wps_xml.status_to_int(metadata.ProcessSucceeded()), result=result.xml())

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

@shared_task
def execute(instance_id, identifier, data_inputs, hostname, port):
    """ Handles an execute request. """
    try:
        instance = models.Instance.objects.get(pk=instance_id)
    except models.Instance.DoesNotExist:
        logger.info('Instance id "%s" does not exist', instance_id)

        return

    server = default_server()

    logger.info('Executing %s on CDAS2 instance at %s:%s',
            identifier, instance.host, instance.request)

    job = create_job(server)

    status_location = create_status_location(hostname, job.id, port)

    response = wps_xml.create_execute_response(
            status_location=status_location,
            status=metadata.ProcessStarted(),
            identifier=identifier)

    status = job.status_set.all().latest('created_date')

    status.result = response.xml()

    status.save()

    with closing(create_socket(instance.host, instance.request, zmq.PUSH)) as request:
        request.send(str('{2}!execute!{0}!{1}'.format(identifier, data_inputs, job.id)))

    return response.xml()
