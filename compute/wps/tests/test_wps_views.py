#! /usr/bin/env python

import random

import cwt
import mock
from django import test

from . import helpers
from wps import metrics
from wps import models
from wps.views import service

class WPSViewsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json', 'jobs.json']

    @mock.patch('wps.backends.Backend.get_backend')
    def test_wps_execute_unknown_backend(self, mock_get_backend):
        mock_get_backend.return_value = None

        data = {
            'service': 'WPS',
            'request': 'Execute',
            'identifier': 'CDAT.subset',
            'datainputs': '[variable=[];domain=[];operation=[]]',
            'api_key': 'abcd1234',
        }

        response = self.client.get('/wps/', data)

        self.assertContains(response, 'ows:ExceptionReport')

    @mock.patch('wps.backends.Backend.get_backend')
    def test_wps_execute_with_api_key(self, mock_get_backend):
        mock_backend = mock.MagicMock()

        mock_get_backend.return_value = mock_backend

        data = {
            'service': 'WPS',
            'request': 'Execute',
            'identifier': 'CDAT.subset',
            'datainputs': 'variable=[];domain=[];operation=[]',
        }

        response = self.client.get('/wps/', data, HTTP_COMPUTE_TOKEN='abcd1234')

        self.assertContains(response, 'wps:ExecuteResponse')

        mock_backend.execute.assert_called()

    #@mock.patch('wps.backends.Backend.get_backend')
    #def test_wps_execute_post(self, mock_get_backend):
    #    metrics.jobs_queued = mock.MagicMock(return_value=2)

    #    variable = cwt.wps.data_input('variable', 'variable', '{}')
    #    operation = cwt.wps.data_input('operation', 'operation', '{}')
    #    domain = cwt.wps.data_input('domain', 'domain', '{}')

    #    cwt.bds.reset()

    #    execute_request = cwt.wps.execute('CDAT.subset', '1.0.0', [variable, domain, operation])

    #    response = self.client.post('/wps/?api_key=abcd1234', execute_request.toxml(bds=cwt.bds), content_type='text\\xml')

    #    self.assertContains(response, 'wps:ExecuteResponse')

    def test_wps_execute_unknown_user(self):
        data = {
            'service': 'WPS',
            'request': 'Execute',
            'identifier': 'CDAT.subset',
            'datainputs': 'variable=[];domain=[];operation=[]',
        }

        response = self.client.get('/wps/', data, HTTP_COMPUTE_TOKEN='sakdjlasjdlkasda')

        self.assertContains(response, 'Missing API key for WPS execute request')

    def test_wps_execute(self):
        data = {
            'service': 'WPS',
            'request': 'Execute',
            'identifier': 'CDAT.subset',
            'datainputs': 'variable=[];domain=[];operation=[]',
        }

        response = self.client.get('/wps/', data)

        expected = 'ows:ExceptionReport'

        self.assertContains(response, expected)

    def test_wps_describe_process_multiple(self):
        data = {
            'service': 'WPS',
            'request': 'DescribeProcess',
            'identifier': 'CDAT.subset,CDAT.aggregate',
        }

        response = self.client.get('/wps/', data)

        self.assertContains(response, 'wps:ProcessDescriptions')
        self.assertContains(response, 'wps:ProcessDescription', count=2)

    def test_wps_describe_process(self):
        data = {
            'service': 'WPS',
            'request': 'DescribeProcess',
            'identifier': 'CDAT.subset',
        }

        response = self.client.get('/wps/', data)

        self.assertContains(response, 'wps:ProcessDescriptions')
        self.assertContains(response, 'wps:ProcessDescription', count=2)

    def test_wps_get_capabilities(self):
        data = {
            'service': 'WPS',
            'request': 'GetCapabilities',
        }

        response = self.client.get('/wps/', data)

        self.assertContains(response, 'wps:Capabilities')

    def test_wps(self):
        response = self.client.get('/wps/')

        self.assertContains(response, 'ows:ExceptionReport')

    def test_status_job_does_not_exist(self):
        with self.assertRaises(service.WPSError) as e:
            self.client.get('/api/status/1000000/')

    def test_status(self):
        job = models.Job.objects.first()

        response = self.client.get('/api/status/{}/'.format(job.id))

        self.assertEqual(response.status_code, 200)
