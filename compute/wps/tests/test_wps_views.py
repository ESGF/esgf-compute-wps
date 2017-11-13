#! /usr/bin/env python

import random

import mock
from django import test

from wps import models

class WPSViewsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json', 'jobs.json']

    def setUp(self):
        self.wps_user = models.User.objects.all()[0]

        self.job = models.Job.objects.all()[0]

    @mock.patch('wps.views.wps_service.backends.Backend.get_backend')
    def test_execute_auth(self, backend_mock):
        self.client.login(username=self.wps_user.username, password=self.wps_user.username)

        params = {
            'process': 'CDAT.subset',
            'variable': 'tas',
            'files': [
                'file://file1',
                'file://file2'
            ],
            'regrid': None
        }

        response = self.client.post('/wps/execute/', params)

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('report', data['data'])

    def test_execute_missing_required(self):
        self.client.login(username=self.wps_user.username, password=self.wps_user.username)

        response = self.client.post('/wps/execute/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Missing required parameter "\'process\'"')

    def test_execute(self):
        response = self.client.post('/wps/execute/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_generate_auth(self):
        self.client.login(username=self.wps_user.username, password=self.wps_user.username)

        params = {
            'process': 'CDAT.subset',
            'variable': 'tas',
            'files': [
                'file://file1',
                'file://file2'
            ],
            'regrid': None
        }

        response = self.client.post('/wps/generate/', params)

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data'], {u'text': u"import cwt\nimport time\n\nwps = cwt.WPS('http://0.0.0.0:8000/wps', api_key='')\n\nfiles = [\n\tcwt.Variable('file://file2', 'tas'),\n]\n\nproc = wps.get_process('CDAT.subset')\n\nwps.execute(proc, inputs=files)\n\nwhile proc.processing:\n\tprint proc.status\n\n\ttime.sleep(1)\n\nprint proc.status", u'filename': u'subset.py'})

    def test_generate_missing_required(self):
        self.client.login(username=self.wps_user.username, password=self.wps_user.username)

        response = self.client.post('/wps/generate/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Missing required key "\'process\'"')

    def test_generate(self):
        response = self.client.post('/wps/generate/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_get_capabilities(self):
        response = self.client.get('/wps/?request=GetCapabilities&service=WPS')

        self.assertEqual(response.status_code, 200)

    def test_generate_capabilities_auth(self):
        self.wps_user.is_superuser = True
        self.wps_user.save()

        self.client.login(username=self.wps_user.username, password=self.wps_user.username)

        with self.assertNumQueries(5):
            response = self.client.get('/wps/regen_capabilities/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data'], 'Regenerated capabilities')

    def test_generate_capabilities_forbidden(self):
        self.client.login(username=self.wps_user.username, password=self.wps_user.username)

        with self.assertNumQueries(2):
            response = self.client.get('/wps/regen_capabilities/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Forbidden access')

    def test_generate_capabilities(self):
        with self.assertNumQueries(0):
            response = self.client.get('/wps/regen_capabilities/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_status(self):
        response = self.client.get('/wps/status/{}/'.format(self.job.id))

        self.assertEqual(response.status_code, 200)
