#! /usr/bin/env python

import json
import logging
import os
import time

import django
import redis
from esgf.wps_lib import metadata
from esgf.wps_lib import operations
from lxml import etree

from wps import models
from wps import tasks
from wps.conf import settings

logger = logging.getLogger(__name__)

class NodeManagerError(Exception):
    pass

class NodeManager(object):

    def connect_redis(self):
        """ Create redis connection from environment variables. """
        host = os.getenv('REDIS_HOST', '0.0.0.0')

        port = os.getenv('REDIS_PORT', 6379)

        db = os.getenv('REDIS_DB', 0)

        return redis.Redis(host, port, db)

    def initialize(self):
        """ Initialize node_manager

        Only run if WPS_NO_INIT is not set.
        """
        if os.getenv('WPS_NO_INIT') is not None:
            return

        redis = self.connect_redis()

        # Enable this to force a initialization each startup
        #redis.delete('init')

        if redis.get('init') is not None:
            return 

        logger.info('Initializing node manager')

        try:
            instances = models.Instance.objects.all()

            if len(instances) > 0:
                # Assume all instances have same capabilities
                tasks.instance_capabilities.delay(instances[0].id)

                # Wait until task worker is done grabbing capabilities
                while redis.get('init') is None:
                    time.sleep(1)

                logger.info('Initialization done')
                
                time.sleep(10)
            else:
                logger.info('No CDAS instances were found to querying capabilities')
        except django.db.utils.ProgrammingError:
            logger.info('Database does not appear to be setup yet, skipping initialization')

            return

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
                    'Job with id %s does not exist', job_id)
        else:
            return job.result

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
            server = models.Server.objects.get(host='0.0.0.0')
        except models.Server.DoesNotExist:
            raise self.create_wps_exception(
                    metadata.NoApplicableCode,
                    'Default server has not been created yet')

        return server.capabilities

    def handle_describe_process(self, identifier):
        """ Handles describe_process operation. """
        pass

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
            #TODO implement describe process, will be the same besides identifier
            raise NotImplementedError()
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
