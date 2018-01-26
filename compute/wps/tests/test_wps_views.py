#! /usr/bin/env python

import random

import cwt
import mock
from cwt.wps_lib import metadata
from cwt.wps_lib import operations
from django import test

from . import helpers
from wps import models
from wps.views import wps_service

class WPSViewsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json', 'jobs.json']

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
        op.set_inputs(*variables.values())
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

    def test_wps_execute_missing_datainputs(self):
        response = self.client.get('/wps/', {'request': 'Execute', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.subset'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')
        self.assertIn('datainputs', response.content)

    def test_wps_execute_no_api_key(self):
        response = self.client.get('/wps/', {'request': 'Execute', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.subset', 
                                             'datainputs': ''})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_execute_missing_backend(self):
        models.Process.objects.create(identifier='CDAT.new', backend='something new')

        user = models.User.objects.first()

        user.auth.api_key = 'new_key'

        user.auth.save()

        datainputs = '[variable=[{"id":"tas|tas","uri":"file:///test.nc"}];domain=[];operation=[{"name":"CDAT.new","input":["tas"]}]]'

        response = self.client.get('/wps/', {'request': 'Execute', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.new', 
                                             'datainputs': datainputs, 
                                             'api_key': 'new_key'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_execute_workflow_invalid_dangling(self):
        user = models.User.objects.first()

        user.auth.api_key = 'new_key'

        user.auth.save()

        datainputs = '[variable=[{"id":"tas|tas","uri":"file:///test.nc"}];domain=[];operation=[{"name":"CDAT.aggregate","input":["tas"],"result":"aggregate"},{"name":"CDAT.subset","input":["tas"],"result":"subset"}]]'

        response = self.client.get('/wps/', {'request': 'Execute', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.subset', 
                                             'datainputs': datainputs, 
                                             'api_key': 'new_key'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_execute_workflow(self):
        user = models.User.objects.first()

        user.auth.api_key = 'new_key'

        user.auth.save()

        datainputs = '[variable=[{"id":"tas|tas","uri":"file:///test.nc"}];domain=[];operation=[{"name":"CDAT.aggregate","input":["tas"],"result":"aggregate"},{"name":"CDAT.subset","input":["aggregate"],"result":"subset"}]]'

        response = self.client.get('/wps/', {'request': 'Execute', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.subset', 
                                             'datainputs': datainputs, 
                                             'api_key': 'new_key'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_execute_error_parsing(self):
        user = models.User.objects.first()

        user.auth.api_key = 'new_key'

        user.auth.save()

        datainputs = ''

        response = self.client.get('/wps/', {'request': 'Execute', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.subset', 
                                             'datainputs': datainputs, 
                                             'api_key': 'new_key'})

        print 'REALLY', response.content

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_execute_missing_operation(self):
        user = models.User.objects.first()

        user.auth.api_key = 'new_key'

        user.auth.save()

        datainputs = '[variable=[{"id":"tas|tas","uri":"file:///test.nc"}];domain=[];operation=[]]'

        response = self.client.get('/wps/', {'request': 'Execute', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.subset', 
                                             'datainputs': datainputs, 
                                             'api_key': 'new_key'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_execute_process_missing(self):
        user = models.User.objects.first()

        user.auth.api_key = 'new_key'

        user.auth.save()

        datainputs = '[variable=[{"id":"tas|tas","uri":"file:///test.nc"}];domain=[];operation=[{"name":"CDAT.subset","input":["tas"]}]]'

        response = self.client.get('/wps/', {'request': 'Execute', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.doesnotexist', 
                                             'datainputs': datainputs, 
                                             'api_key': 'new_key'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_execute_post(self):
        user = models.User.objects.first()

        user.auth.api_key = 'new_key'

        user.auth.save()

        variable = metadata.Input(identifier='variable', data=metadata.ComplexData(value='[{"id":"tas|tas","uri":"file:///test.nc"}]'))

        domain = metadata.Input(identifier='domain', data=metadata.ComplexData(value='[]'))

        operation = metadata.Input(identifier='operation', data=metadata.ComplexData(value='[{"name":"CDAT.subset","input":["tas"]}]'))
    
        datainputs = operations.ExecuteRequest(service='WPS', version='1.0.0', identifier='CDAT.subset', data_inputs=[variable, domain, operation])

        response = self.client.post('/wps/', datainputs.xml(), content_type='text/xml')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_execute_get(self):
        user = models.User.objects.first()

        user.auth.api_key = 'new_key'

        user.auth.save()

        datainputs = '[variable=[{"id":"tas|tas","uri":"file:///test.nc"}];domain=[];operation=[{"name":"CDAT.subset","input":["tas"]}]]'

        response = self.client.get('/wps/', {'request': 'Execute', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.subset', 
                                             'datainputs': datainputs, 
                                             'api_key': 'new_key'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_describe_process(self):
        response = self.client.get('/wps/', {'request': 'DescribeProcess', 
                                             'service': 'WPS', 
                                             'identifier': 'CDAT.subset'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_get_capabilities(self):
        # TODO maybe need to call a function to generate
        user = models.User.objects.first()

        user.is_superuser = True

        user.save()

        self.client.login(username=user.username, password=user.username)

        self.client.get('/wps/regen_capabilities/')

        response = self.client.get('/wps/', {'request': 'GetCapabilities', 'service': 'WPS'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')
        self.assertNotEqual(response.content, '')

    def test_wps_post(self):
        response = self.client.post('/wps/')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')

    def test_wps_get(self):
        response = self.client.get('/wps/')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'text/xml')

        #TODO verify that the response is correctly formatted WPS response

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
