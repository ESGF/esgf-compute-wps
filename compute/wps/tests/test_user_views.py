import random

from django import test

from wps import models

class UserViewsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'files.json']

    def setUp(self):
        self.update = models.User.objects.all()[0]
        
    def test_user_stats_process_auth(self):
        self.client.login(username=self.update.username, password=self.update.username)

        response = self.client.get('/auth/user/stats/', {'stat': 'process'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('processes', data['data'])
        self.assertEqual(len(data['data']['processes']), 6)

    def test_user_stats_process(self):
        response = self.client.get('/auth/user/stats/', {'type': 'process'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_user_stats_files_auth(self):
        self.client.login(username=self.update.username, password=self.update.username)

        response = self.client.get('/auth/user/stats/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('files', data['data'])
        self.assertEqual(len(data['data']['files']), 133)

    def test_user_stats_files(self):
        response = self.client.get('/auth/user/stats/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_user_details_auth(self):
        self.client.login(username=self.update.username, password=self.update.username)

        response = self.client.get('/auth/user/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')

        expected = ('username', 'openid', 'admin', 'local_init', 'api_key', 'type', 'email')

        for exp in expected:
            self.assertIn(exp, data['data'])

    def test_user_details(self):
        response = self.client.get('/auth/user/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_update_auth(self):
        self.client.login(username=self.update.username, password=self.update.username)

        params = {
            'email': 'imdifferent@hello.com',
            'openid': 'http://test',
            'password': 'test2'
        }

        response = self.client.post('/auth/update/', params)

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')

        expected = ('username', 'openid', 'admin', 'local_init', 'api_key', 'type', 'email')

        for exp in expected:
            self.assertIn(exp, data['data'])

            if exp in params:
                self.assertEqual(params[exp], data['data'][exp])

    def test_update(self):
        response = self.client.post('/auth/update/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_regenerate_auth(self):
        self.client.login(username=self.update.username, password=self.update.username)

        response = self.client.get('/auth/user/regenerate/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Initial API key has not been generate yet, authenticate with MyProxyClient or OAuth2')

    def test_regenerate(self):
        response = self.client.get('/auth/user/regenerate/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
