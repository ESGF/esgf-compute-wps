#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import json
import logging
import os
import random
import re
import string
import tempfile
import time
from contextlib import closing

import cwt
import django
import redis
import zmq
from celery import group
from cwt import wps_lib
from lxml import etree
from myproxy.client import MyProxyClient
from openid.consumer import discover

from wps import models
from wps import settings
from wps import tasks
from wps import wps_xml
from wps.auth import oauth2
from wps.processes import get_process
from wps.backends import backend

logger = logging.getLogger('wps.node_manager')

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_ACCESS = 'urn:esg:security:oauth:endpoint:access'
URN_RESOURCE = 'urn:esg:security:oauth:endpoint:resource'
URN_MPC = 'urn:esg:security:myproxy-service'

discover.OpenIDServiceEndpoint.openid_type_uris.extend([
    URN_AUTHORIZE,
    URN_ACCESS,
    URN_RESOURCE,
    URN_MPC
])

def openid_find_service_by_type(services, uri):
    for s in services:
        if uri in s.type_uris:
            return s

    return None

class NodeManagerError(Exception):
    pass

class NodeManagerWPSError(Exception):
    def __init__(self, exc_type, message):
        self.exc_report = wps_lib.ExceptionReport(settings.VERSION)

        self.exc_report.add_exception(exc_type, message)

class NodeManager(object):

    def regenerate_api_key(self, user_id):
        try:
            user = models.User.objects.get(pk=user_id)
        except models.User.DoesNotExist:
            raise Exception('User does not exist')

        if user.auth.api_key == '':
            raise Exception('Need to generate an api_key, log into either OAuth2 or MyProxyClient')

        user.auth.api_key = ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(64))

        user.auth.save()

        logger.info('Regenerated API_KEY for user {}'.format(user.username))

        return user.auth.api_key

    def create_socket(self, host, port, socket_type):
        """ Create a ZMQ socket. """
        context = zmq.Context.instance()

        socket = context.socket(socket_type)

        socket.connect('tcp://{0}:{1}'.format(host, port))

        return socket

    def update_user(self, service, oid_url, certs, **extra):
        """ Create a new user. """
        try:
            user = models.User.objects.get(auth__openid_url=oid_url)
        except models.User.DoesNotExist:
            raise Exception('User does not exist')

        if user.auth.api_key == '':
            user.auth.api_key = ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(64))

        user.auth.type = service
        user.auth.cert = ''.join(certs)
        user.auth.extra = json.dumps(extra)
        user.auth.save()

        logger.info('Updated auth settings for user {}'.format(user.username))

    def auth_mpc(self, oid_url, username, password):
        url, services = discover.discoverYadis(oid_url)

        mpc_service = openid_find_service_by_type(services, URN_MPC)

        g = re.match('socket://(.*):(.*)', mpc_service.server_url)

        if g is None:
            raise Exception('Failed to parse MyProxyClient endpoint')

        host, port = g.groups()

        m = MyProxyClient(hostname=host, caCertDir=settings.CA_PATH)

        c = m.logon(username, password, bootstrap=True)

        logger.info('Authenticated with MyProxyClient backend for user {}'.format(username))

        self.update_user('myproxyclient', oid_url, c)

    def auth_oauth2(self, oid_url):
        url, services = discover.discoverYadis(oid_url)

        auth_service = openid_find_service_by_type(services, URN_AUTHORIZE)

        cert_service = openid_find_service_by_type(services, URN_RESOURCE)

        redirect_url, state = oauth2.get_authorization_url(auth_service.server_url, cert_service.server_url)

        logger.info('Retrieved authorization url for OpenID {}'.format(oid_url))

        session = {
            'oauth_state': state,
            'openid': oid_url,
        }

        return redirect_url, session

    def auth_oauth2_callback(self, oid_url, query, state):
        url, services = discover.discoverYadis(oid_url)

        token_service = openid_find_service_by_type(services, URN_ACCESS)

        cert_service = openid_find_service_by_type(services, URN_RESOURCE)

        request_url = '{}?{}'.format(settings.OAUTH2_CALLBACK, query)

        token = oauth2.get_token(token_service.server_url, request_url, state)

        logger.info('Retrieved OAuth2 token for OpenID {}'.format(oid_url))

        cert, key, new_token = oauth2.get_certificate(token, token_service.server_url, cert_service.server_url)

        logger.info('Retrieved Certificated for OpenID {}'.format(oid_url))

        self.update_user('oauth2', oid_url, [cert, key], token=new_token)

    def get_parameter(self, params, name):
        """ Gets a parameter from a django QueryDict """

        # Case insesitive
        temp = dict((x.lower(), y) for x, y in params.iteritems())

        if name.lower() not in temp:
            logger.info('Missing required parameter %s', name)

            raise NodeManagerWPSError(wps_lib.MissingParameterValue, name)

        return temp[name.lower()]

    def get_status(self, job_id):
        """ Get job status. """
        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise NodeManagerError('Job {0} does not exist'.format(job_id))

        return job.report

    def generate_capabilities(self):
        """ Generates capabilites for each server. """
        servers = models.Server.objects.all()

        for s in servers:
            logger.info('Generating capabilities for server {}'.format(s.host))

            processes = s.processes.all()

            cap = wps_xml.capabilities_response(add_procs=processes)

            s.capabilities = cap.xml()

            s.save()

    def cdas2_capabilities(self):
        """ Queries for CDAS capabilities. """
        servers = models.Server.objects.all()

        for s in servers:
            logger.info('Querying capabilities for CDAS backend {}'.format(s.host))

            tasks.capabilities.delay(s.id)

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
            raise Exception('Process "{}" does not exist.'.format(identifier))

        return process.description

    def execute(self, user, identifier, data_inputs):
        """ WPS execute operation """
        try:
            process = models.Process.objects.get(identifier=identifier)
        except models.Process.DoesNotExist:
            raise Exception('Process "{}" does not exist.'.format(identifier))

        process.track(user)

        operations, domains, variables = cwt.WPS.parse_data_inputs(data_inputs)

        for variable in variables:
            models.File.track(user, variable)

        server = models.Server.objects.get(host='default')

        job = models.Job.objects.create(server=server, user=user, process=process, extra=data_inputs)

        job.accepted()

        logger.info('Accepted job {}'.format(job.id))

        process_backend = backend.Backend.get_backend(process.backend)

        if process_backend is None:
            job.failed()

            raise Exception('Process backend "{}" does not exist'.format(process.backend))

        operation_dict = dict((x.name, x.parameterize()) for x in operations)

        domain_dict = dict((x.name, x.parameterize()) for x in domains)

        variable_dict = dict((x.name, x.parameterize()) for x in variables)

        process_backend.execute(identifier, variable_dict, domain_dict, operation_dict, user=user, job=job)

        return job.report

    def handle_get(self, params):
        """ Handle an HTTP GET request. """
        request = self.get_parameter(params, 'request')

        service = self.get_parameter(params, 'service')

        api_key = params.get('api_key')

        operation = request.lower()

        identifier = None

        data_inputs = None

        if operation == 'describeprocess':
            identifier = self.get_parameter(params, 'identifier')
        elif operation == 'execute':
            identifier = self.get_parameter(params, 'identifier')

            data_inputs = self.get_parameter(params, 'datainputs')

        return api_key, operation, identifier, data_inputs

    def handle_post(self, data, params):
        """ Handle an HTTP POST request. 

        NOTE: we only support execute requests as POST for the moment
        """
        try:
            request = wps_lib.ExecuteRequest.from_xml(data)
        except etree.XMLSyntaxError:
            logger.exception('Failed to parse xml request')

            raise Exception('POST request only supported for Execure operation')

        # Build to format [variable=[];domain=[];operation=[]]
        data_inputs = '[{0}]'.format(
            ';'.join('{0}={1}'.format(x.identifier, x.data.value) 
                     for x in request.data_inputs))

        # CDAS doesn't like single quotes
        data_inputs = data_inputs.replace('\'', '\"')

        api_key = params.get('api_key')

        return api_key, 'execute', request.identifier, data_inputs

    def handle_request(self, request):
        """ Convert HTTP request to intermediate format. """
        if request.method == 'GET':
            return self.handle_get(request.GET)
        elif request.method == 'POST':
            return self.handle_post(request.body, request.GET)
