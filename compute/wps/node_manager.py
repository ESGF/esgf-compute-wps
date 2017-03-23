#! /usr/bin/env python

import json
import logging
import os
import time

import django
import redis
from cwt.wps_lib import metadata
from cwt.wps_lib import operations
from lxml import etree

from wps import models
from wps import tasks
from wps.conf import settings

logger = logging.getLogger(__name__)

class NodeManagerError(Exception):
    pass

class NodeManager(object):

    def initialize(self):
        """ Initialize node_manager

        Only run if WPS_NO_INIT is not set.
        """
        if os.getenv('WPS_NO_INIT') is not None:
            return

        logger.info('Initializing node manager')

        try:
            server = models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            logger.info('Default server does not exist.')

            return
        except django.db.utils.ProgrammingError:
            logger.info('Database has not been initialized yet.')

            return

        if server.capabilities == '':
            logger.info('Servers capabilities have not been populated')

            instances = models.Instance.objects.all()

            if len(instances) == 0:
                logger.info('No CDAS2 instances have been registered with the server.')

                return

            tasks.capabilities.delay(server.id, instances[0].id)

            logger.info('Sent capabilities query to CDAS2 instance %s:%s',
                    instances[0].host,
                    instances[0].request)

    def create_wps_exception(self, ex_type, message):
        """ Create an ExceptionReport. """
        ex_report = metadata.ExceptionReport(settings.WPS_VERSION)

        ex_report.add_exception(ex_type, message)

        return NodeManagerError(ex_report.xml())

    def get_parameter(self, params, name):
        """ Gets a parameter from a django QueryDict """

        # Case insesitive
        temp = dict((x.lower(), y) for x, y in params.iteritems())

        if name.lower() not in temp:
            logger.info('Missing required parameter %s', name)

            raise self.create_wps_exception(
                    metadata.MissingParameterValue,
                    name)

        return temp[name.lower()]

    def get_status(self, job_id):
        """ Get job status. """
        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise self.create_wps_exception(
                    metadata.NoApplicableCode,
                    'Job with id {0} does not exist'.format(job_id))

        try:
            latest_status = job.status_set.all().latest('created_date')
        except models.Status.DoesNotExist:
            raise NodeManagerError('Job {0} has not states'.format(job_id))

        return latest_status.result

    def get_instance(self):
        """ Determine which CDAS instance to execute on. """
        instances = models.Instance.objects.all()

        if len(instances) == 0:
            raise self.create_wps_exception(
                    metadata.NoApplicableCode,
                    'No CDAS2 instances are available')

        return instances[0]

    def handle_get_capabilities(self):
        """ Handles get_capabilities operation. """
        logger.info('Handling GetCapabilities request')

        try:
            server = models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            raise self.create_wps_exception(
                    metadata.NoApplicableCode,
                    'Default server has not been created yet')

        if server.capabilities == '':
            raise self.create_wps_exception(
                    metadata.NoApplicableCode,
                    'Servers capabilities have not been populated yet, server may still be starting up')

        return server.capabilities

    def handle_describe_process(self, identifier):
        """ Handles describe_process operation. """
        logger.info('Handling DescribeProcess request')
        
        try:
            process = models.Process.objects.get(identifier=identifier)
        except models.Process.DoesNotExist:
            raise self.create_wps_exception(
                    metadata.NoApplicableCode,
                    'Process {0} does not exist'.format(identifier))

        return process.description

    def handle_execute(self, identifier, data_inputs):
        """ Handles execute operation """
        logger.info('Handling Execute request')

        instance = self.get_instance()

        logger.info('Executing on CDAS2 instance %s:%s', instance.host, instance.request)

        task = tasks.execute.delay(instance.id, identifier, data_inputs)

        response = task.get()

        return response

    def handle_get(self, params):
        """ Handle an HTTP GET request. """
        logger.info('Received GET request %s', params)
        
        request = self.get_parameter(params, 'request')

        service = self.get_parameter(params, 'service')

        request = request.lower()

        if request == 'getcapabilities':
            response = self.handle_get_capabilities()
        elif request == 'describeprocess':
            identifier = self.get_parameter(params, 'identifier')

            response = self.handle_describe_process(identifier)
        elif request == 'execute':
            identifier = self.get_parameter(params, 'identifier')

            data_inputs = self.get_parameter(params, 'datainputs')

            response = self.handle_execute(identifier, data_inputs)

        return response

    def handle_post(self, data):
        """ Handle an HTTP POST request. 

        NOTE: we only support execute requests as POST for the moment
        """
        logger.info('Received POST request %s', data)

        try:
            request = operations.ExecuteRequest.from_xml(data)
        except etree.XMLSyntaxError:
            logger.exception('Failed to parse xml request')

            raise self.create_wps_exception(
                    metadata.NoApplicableCode,
                    'POST request only supported for Execute operation')

        # Build to format [variable=[];domain=[];operation=[]]
        data_inputs = '[{0}]'.format(
                ';'.join('{0}={1}'.format(x.identifier, x.data.value)
                    for x in request.data_inputs))

        # CDAS doesn't like single quotes
        data_inputs = data_inputs.replace('\'', '\"')

        response = self.handle_execute(request.identifier, data_inputs)
        
        return response

    def handle_request(self, request):
        """ Handle HTTP request """
        if request.method == 'GET':
            return self.handle_get(request.GET)
        elif request.method == 'POST':
            return self.handle_post(request.body)
