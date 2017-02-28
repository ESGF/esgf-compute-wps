#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

from esgf.wps_lib import metadata
from esgf.wps_lib import operations
from celery.signals import celeryd_init
from celery.utils.log import get_task_logger
from celery import shared_task

from wps import node_manager as node

logger = get_task_logger(__name__)

@celeryd_init.connect
def start_monitor_tasks(sender=None, instance=None, **kwargs):
    monitor_responses.delay()

@shared_task
def monitor_responses():
    logger.info('Monitor task started')

    manager = node.NodeManager()

    manager.monitor_responses()

@shared_task
def handle_get(params):
    logger.info('Handling GET request with params: %s', params)

    manager = node.NodeManager()    

    request = params.get('request').lower()

    if request == 'getcapabilities':
        result = manager.get_capabilities()
    elif request == 'describeprocess':
        identifier = params.get('identifier')

        logger.debug('Executing process %s', identifier)

        result = manager.describe_process(identifier)
    elif request == 'execute':
        identifier = params.get('Identifier')

        data_inputs = params.get('datainputs')

        logger.debug('Executing process %s', identifier)

        job_id = manager.execute(identifier, data_inputs)

        # This should be loaded from database
        process = metadata.Process() 
        process.identifier = 'dummy'
        process.title = 'dummy'

        # Alot of values are static so we'll store a copy a load and 
        # make deep copies as needed
        response = operations.ExecuteResponse()
        response.service = 'WPS'
        response.version = '1.0.0'
        response.lang = 'en-CA'
        response.service_instance = 'http://aims2.llnl.gov/wps'
        response.creation_time = '{0: %X} {0: %x}'.format(datetime.datetime.now())
        response.status_location = 'http://wps:8000/wps/status?job_id={0}'.format(job_id)
        response.status = metadata.ProcessStarted()
        response.process = process

        result = response.xml()

    return result

@shared_task
def handle_post(request):
    pass
