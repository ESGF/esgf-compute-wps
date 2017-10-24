#! /usr/bin/env python

import random

from wps import models

from .common import CommonTestCase

class WPSViewsTestCase(CommonTestCase):

    def setUp(self):
        self.wps_user = models.User.objects.create_user('wps', 'wps@gmail.com', 'wps')

        models.Auth.objects.create(user=self.wps_user)

        self.job = models.Job.objects.create(server=self.server,
                                        user=self.wps_user,
                                        process=random.choice(self.processes))

        self.job.accepted()

        self.job.started()

        self.job.succeeded('success')

    def tearDown(self):
        self.wps_user.delete()

        self.job.delete()

    def test_execute_auth(self):
        self.client.login(username='wps', password='wps')

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

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Process "CDAT.subset" does not exist.')

    def test_execute_missing_required(self):
        self.client.login(username='wps', password='wps')

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
        self.client.login(username='wps', password='wps')

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
        self.client.login(username='wps', password='wps')

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

        self.client.login(username='wps', password='wps')

        with self.assertNumQueries(5):
            response = self.client.get('/wps/regen_capabilities/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data'], 'Regenerated capabilities')

    def test_generate_capabilities_forbidden(self):
        self.client.login(username='wps', password='wps')

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
        response = self.client.get('/wps/job/{}/'.format(self.job.id))

        self.assertEqual(response.status_code, 200)
