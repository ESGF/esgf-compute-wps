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

def create_status_location(host, job_id, port=None):
    loc = 'http://{0}'.format(host)

    if port is not None:
        loc = '{0}:{1}'.format(loc, port)

    loc = '{0}/wps/job/{1}'.format(loc, job_id)

    return loc

def create_socket(host, port, socket_type):
    context = zmq.Context.instance()

    socket = context.socket(socket_type)

    socket.connect('tcp://{0}:{1}'.format(host, port))

    return socket

@celeryd_init.connect
def monitor_handler(**kwargs):
    logger.info('celeryd_init, starting monitors')

    instances = models.Instance.objects.all()

    try:
        for i in instances:
            monitor_cdas.delay(i.id)
    except django.db.utils.ProgrammingError:
        logger.info('Database does not appear to be setup, not starting monitors')

@shared_task
def handle_response(data):
    job_id, _, response = data.split('!')

    logger.info('Handling CDAS2 response for job %s', job_id)

    try:
        result = wps_xml.convert_cdas2_response(response)
    except Exception:
        logger.exception('Failed to convert CDAS2 response')

        return

    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        logger.exception('Job %s does not exist', job_id)

        return

    if isinstance(result, operations.GetCapabilitiesResponse):
        job.server.capabilities = result.xml()

        job.server.save()

        describe.starmap((job.server.id, x.identifier)
                for x in result.process_offerings).delay()
    elif isinstance(result, operations.DescribeProcessResponse):
        result = wps_xml.create_describe_process_response(response)
        
        try:
            server = models.Server.objects.get(host='0.0.0.0')
        except models.Server.DoesNotExist:
            logger.expcetion('Default server does not exist')

            return

        process = models.Process(
                identifier=result.process_description[0].identifier,
                description=result.xml())
        
        process.save()

        server.processes.add(process)
    elif isinstance(result, operations.ExecuteResponse):
        latest_response = job.status_set.all().latest('created_date')

        result = wps_xml.update_execute_response(latest_response.result, response)

    job.status_set.create(status=wps_xml.status_to_int(metadata.ProcessSucceeded()), result=result.xml())
    
@shared_task
def monitor_cdas(instance_id):
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

def create_job(server, status=None, result=None):
    if status is None:
        status = metadata.ProcessStarted()

    job = models.Job(server=server)

    job.save()

    job.status_set.create(status=wps_xml.status_to_int(status))
    
    return job

@shared_task
def capabilities(server_id, instance_id):
    try:
        instance = models.Instance.objects.get(pk=instance_id)
    except models.Instance.DoesNotExist:
        logger.info('Instance id "%s" does not exist', instance_id)

        return

    logger.info('Querying CDAS instance at %s:%s for capabilities',
            instance.host, instance.request)

    try:
        server = models.Server.objects.get(pk=server_id)
    except models.Server.DoesNotExist:
        logger.info('Server id "%s" does not exist', server_id)

        return

    job = create_job(server)

    with closing(create_socket(instance.host, instance.request, zmq.PUSH)) as request:
        request.send(str('{0}!getCapabilities!WPS'.format(job.id)))

@shared_task
def describe(server_id, identifier):
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

    job = create_job(server)

    with closing(create_socket(instance[0].host, instance[0].request, zmq.PUSH)) as request:
        request.send(str('{0}!describeProcess!{1}'.format(job.id, identifier)))

@shared_task
def execute(instance_id, identifier, data_inputs):
    try:
        instance = models.Instance.objects.get(pk=instance_id)
    except models.Instance.DoesNotExist:
        logger.info('Instance id "%s" does not exist', instance_id)

        return

    try:
        server = models.Server.objects.get(host='0.0.0.0')
    except models.Instance.DoesNotExist:
        logger.info('Default server does not exist yet')

        return

    logger.info('Executing %s on CDAS2 instance at %s:%s',
            identifier, instance.host, instance.request)

    job = create_job(server)

    response = wps_xml.create_execute_response(
            status_location=create_status_location('0.0.0.0', job.id, '8000'),
            status=metadata.ProcessStarted(),
            identifier=identifier)

    status = job.status_set.all().latest('created_date')

    status.result = response.xml()

    status.save()

    with closing(create_socket(instance.host, instance.request, zmq.PUSH)) as request:
        request.send(str('{2}!execute!{0}!{1}'.format(identifier, data_inputs, job.id)))

    return response.xml()
