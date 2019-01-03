#! /usr/bin/env python

import random

import cwt
import mock
from django import test

from . import helpers
from wps import metrics
from wps import models
from wps.views import wps_service

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
            'datainputs': '[variable=[];domain=[];operation=[]]',
            'api_key': 'abcd1234',
        }

        response = self.client.get('/wps/', data)

        self.assertContains(response, 'wps:ExecuteResponse')

        mock_backend.execute.assert_called()

    @mock.patch('wps.backends.Backend.get_backend')
    def test_wps_execute_post(self, mock_get_backend):
        metrics.jobs_queued = mock.MagicMock(return_value=2)

        variable = cwt.wps.data_input('variable', 'variable', '{}')
        operation = cwt.wps.data_input('operation', 'operation', '{}')
        domain = cwt.wps.data_input('domain', 'domain', '{}')

        cwt.bds.reset()

        execute_request = cwt.wps.execute('CDAT.subset', '1.0.0', [variable, domain, operation])

        response = self.client.post('/wps/?api_key=abcd1234', execute_request.toxml(bds=cwt.bds), content_type='text\\xml')

        self.assertContains(response, 'wps:ExecuteResponse')

    def test_wps_execute_unknown_user(self):
        data = {
            'service': 'WPS',
            'request': 'Execute',
            'identifier': 'CDAT.subset',
            'datainputs': '[variable=[];domain=[];operation=[]]',
            'api_key': 'skldjalsjfdlajflk',
        }

        response = self.client.get('/wps/', data)

        self.assertContains(response, 'Missing API key for WPS execute request')

    def test_wps_execute(self):
        data = {
            'service': 'WPS',
            'request': 'Execute',
            'identifier': 'CDAT.subset',
            'datainputs': '[variable=[];domain=[];operation=[]]',
        }

        response = self.client.get('/wps/', data)

        expected = '<?xml version="1.0" ?><ows:ExceptionReport version="1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1"><ows:Exception exceptionCode="MissingParameterValue"><ows:ExceptionText>api_key</ows:ExceptionText></ows:Exception></ows:ExceptionReport>'

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
            'identifier': 'CDAT.subset',
        }

        response = self.client.get('/wps/', data)

        self.assertContains(response, 'wps:Capabilities')

    def test_wps(self):
        response = self.client.get('/wps/')

        self.assertContains(response, 'ows:ExceptionReport')

    def test_script_generator(self):
        user = models.User.objects.first()

        variables = {
            'v0': cwt.Variable('file:///test.nc', 'tas', name='v0'),
            'v1': cwt.Variable('file:///test.nc', 'tas', name='v1'),
        }

        domains = {'d0': cwt.Domain([cwt.Dimension('time', 0, 200)], name='d0')}

        gridder = cwt.Gridder(grid='gaussian~32')

        op = cwt.Process(identifier='CDAT.subset')
        op.domain = domains.values()[0]
        op.add_inputs(*variables.values())
        op.parameters['gridder'] = gridder
        op.parameters['axes'] = cwt.NamedParameter('axes', 'time')

        operations = {'subset': op}

        sg = wps_service.WPSScriptGenerator(variables, domains, operations, user)

        data = sg.generate()

        self.assertIsNotNone(data)

    def test_wps_generate_missing_authentication(self):
        datainputs = '[variable=[{"id":"tas|tas","uri":"file:///test.nc"}];domain=[];operation=[{"name":"CDAT.subset","input":["tas"]}]]'

        response = self.client.post('/wps/generate/', {'datainputs': datainputs})

        helpers.check_failed(self, response)

    def test_wps_generate(self):
        user = models.User.objects.first()

        self.client.login(username=user.username, password=user.username)

        datainputs = '[variable=[{"id":"tas|tas","uri":"file:///test.nc"}];domain=[];operation=[{"name":"CDAT.subset","input":["tas"]}]]'

        response = self.client.post('/wps/generate/', {'datainputs': datainputs})

        data = helpers.check_success(self, response)['data']

        self.assertIn('text', data) 
        self.assertIn('filename', data)

    def test_regen_capabilities_missing_authentication(self):
        response = self.client.get('/wps/regen_capabilities/')

        helpers.check_failed(self, response)

    def test_regen_capabilities_missing_authorization(self):
        user = models.User.objects.first()

        self.client.login(username=user.username, password=user.username)

        response = self.client.get('/wps/regen_capabilities/')

        helpers.check_failed(self, response)

    def test_regen_capabilities(self):
        user = models.User.objects.first()

        user.is_superuser = True

        user.save()

        self.client.login(username=user.username, password=user.username)

        response = self.client.get('/wps/regen_capabilities/')

        helpers.check_success(self, response)

    def test_status_job_does_not_exist(self):
        with self.assertRaises(wps_service.WPSError) as e:
            self.client.get('/wps/status/1000000/')

    def test_status(self):
        job = models.Job.objects.first()

        response = self.client.get('/wps/status/{}/'.format(job.id))

        self.assertEqual(response.status_code, 200)
