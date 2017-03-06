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
from esgf.wps_lib import metadata

from wps import models
from wps import wps_xml

logger = get_task_logger(__name__)

def __create_socket(host, port, socket_type):
    context = zmq.Context.instance()

    socket = context.socket(socket_type)

    socket.connect('tcp://{0}:{1}'.format(host, port))

    return socket

def connect_redis():
    host = os.getenv('REDIS_HOST', '0.0.0.0')
    
    port = os.getenv('REDIS_PORT', 6379)

    db = os.getenv('REDIS_DB', 0)

    return redis.Redis(host, port, db)

@celeryd_init.connect
def monitor_handler(**kwargs):
    logger.info('celeryd_init, starting monitors')

    instances = models.Instance.objects.all()

    try:
        for i in instances:
            monitor_cdas.delay(i.id)
    except django.db.utils.ProgrammingError:
        logger.info('Database does not appear to be setup, not starting monitors')

def init_handler(response):
    buf = response.recv()

    server_id, _, data = buf.split('!')

    logger.info(data)

    capabilities = wps_xml.create_capabilities_response(data) 

    server = models.Server.objects.get(pk=server_id)

    server.capabilities = capabilities

    server.save()

@shared_task
def store_job_result(job_id, data):
    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        logger.info('Result for job %s does not exist', job_id)
    else:
        response = wps_xml.update_execute_response(job.result, data)

        job.result = response

        job.save()
    
@shared_task
def monitor_cdas(instance_id):
    try:
        instance = models.Instance.objects.get(pk=instance_id)
    except models.Instance.DoesNotExist:
        logger.info('Instance id "%s" does not exist', instance_id)

        return

    logger.info('Monitoring CDAS instance at %s:%s', instance.host, instance.response)

    redis = connect_redis()

    init = redis.get('init')

    with closing(__create_socket(instance.host, instance.response, zmq.PULL)) as response:
        if init is None:
            logger.info('Need to initialize')

            init_handler(response)

            logger.info('Done initializing')

            redis.set('init', True)

        while True:
            buf = response.recv()

            logger.info('Received CDAS response, %s', buf)

            job_id, _, data = buf.split('!')

            store_job_result.delay(job_id, data)

@shared_task
def instance_capabilities(instance_id):
    try:
        instance = models.Instance.objects.get(pk=instance_id)
    except models.Instance.DoesNotExist:
        logger.info('Instance id "%s" does not exist', instance_id)

        return

    logger.info('Querying CDAS instance at %s:%s for capabilities',
            instance.host, instance.request)

    with closing(__create_socket(instance.host, instance.request, zmq.PUSH)) as request:
        request.send(str('1!getCapabilities!WPS'))

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

    logger.info('Executing on CDAS2 instance at %s:%s', instance.host, instance.request)

    job = models.Job(server=server)

    job.save()

    with closing(__create_socket(instance.host, instance.request, zmq.PUSH)) as request:
        request.send(str('{2}!execute!{0}!{1}'.format(identifier, data_inputs, job.id)))

    status_location = 'http://0.0.0.0:8000/wps/job/{0}'.format(job.id)

    response = wps_xml.create_execute_response(status_location,
            metadata.ProcessStarted(),
            identifier)
    
    job.result = response

    job.save()

    return response
