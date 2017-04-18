#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import json
import logging
import os
import random
import string
import time

import django
import redis
from cwt.wps_lib import metadata
from cwt.wps_lib import operations
from django.contrib.auth import models as dj_models
from lxml import etree

from wps import models
from wps import settings
from wps import wps_xml

logger = logging.getLogger(__name__)

class NodeManagerError(Exception):
    pass

class NodeManagerWPSError(Exception):
    def __init__(self, exc_type, message):
        self.exc_report = metadata.ExceptionReport(settings.VERSION)

        self.exc_report.add_exception(exc_type, message)

class NodeManager(object):

    def create_user(self, openid_url, token):
        """ Create a new user. """
        user = dj_models.User() 

        user.username = openid_url

        try:
            user.save()
        except django.db.IntegrityError as e:
            raise NodeManagerError('Failed to create user: {}'.format(e.message))

        oauth2 = models.OAuth2()

        oauth2.user = user
        oauth2.openid = openid_url
        oauth2.token_type = token['token_type']
        oauth2.refresh_token = token['refresh_token']
        oauth2.access_token = token['access_token']
        oauth2.scope = json.dumps(token['scope'])
        oauth2.expires_at = datetime.datetime.fromtimestamp(token['expires_at'])
        oauth2.api_key = ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(64))

        oauth2.save()

        return user.oauth2.api_key

    def create_job(self, server, status=None):
        """ Create a job entry. """
        if status is None:
            status = metadata.ProcessStarted()

        job = models.Job(server=server)

        job.save()

        job.status_set.create(status=wps_xml.status_to_int(status))

        return job

    def get_parameter(self, params, name):
        """ Gets a parameter from a django QueryDict """

        # Case insesitive
        temp = dict((x.lower(), y) for x, y in params.iteritems())

        if name.lower() not in temp:
            logger.info('Missing required parameter %s', name)

            raise NodeManagerWPSError(metadata.MissingParameterValue, name)

        return temp[name.lower()]

    def get_status(self, job_id):
        """ Get job status. """
        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise NodeManagerError('Job {0} does not exist'.format(job_id))

        try:
            latest_status = job.status_set.all().latest('created_date')
        except models.Status.DoesNotExist:
            raise NodeManagerError('Job {0} has not states'.format(job_id))

        return latest_status.result

    def get_capabilities(self):
        """ Retrieves WPS GetCapabilities. """
        try:
            server = models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            raise Exception('Default server does not exist')

        if server.capabilities == '':
            raise Exception('Capabilities has not been populated yet')

        return server.capabilities

    def describe_process(self, identifier):
        """ Retrieves WPS DescribeProcess. """
        try:
            process = models.Process.objects.get(identifier=identifier)
        except models.Process.DoesNotExist:
            raise Exception('Process "{}" does not exist.'.format(ientifier))

        return process.description

    def handle_get(self, params):
        """ Handle an HTTP GET request. """
        logger.info('Received GET request %s', params)
        
        request = self.get_parameter(params, 'request')

        service = self.get_parameter(params, 'service')

        operation = request.lower()

        identifier = None

        data_inputs = None

        if operation == 'describeprocess':
            identifier = self.get_parameter(params, 'identifier')
        elif operation == 'execute':
            identifier = self.get_parameter(params, 'identifier')

            data_inputs = self.get_parameter(params, 'datainputs')

        return operation, identifier, data_inputs

    def handle_post(self, data, params):
        """ Handle an HTTP POST request. 

        NOTE: we only support execute requests as POST for the moment
        """
        logger.info('Received POST request %s', data)

        try:
            request = operations.ExecuteRequest.from_xml(data)
        except etree.XMLSyntaxError:
            logger.exception('Failed to parse xml request')

            raise Exception('POST request only supported for Execure operation')

        # Build to format [variable=[];domain=[];operation=[]]
        data_inputs = '[{0}]'.format(
                ';'.join('{0}={1}'.format(x.identifier, x.data.value)
                    for x in request.data_inputs))

        # CDAS doesn't like single quotes
        data_inputs = data_inputs.replace('\'', '\"')

        return 'execute', request.identifier, data_inputs

    def handle_request(self, request):
        """ Convert HTTP request to intermediate format. """
        if request.method == 'GET':
            return self.handle_get(request.GET)
        elif request.method == 'POST':
            return self.handle_post(request.body, request.GET)
