import random

from wps import models

from .common import CommonTestCase

class UserViewsTestCase(CommonTestCase):

    def setUp(self):
        self.update = models.User.objects.create_user('update', 'update@gmail.com', 'update')

        models.Auth.objects.create(user=self.update)

        for _ in xrange(10):
            file_obj = random.choice(self.files)

            models.UserFile.objects.create(user=self.update, file=file_obj)

        for _ in xrange(10):
            process_obj = random.choice(self.processes)

            models.UserProcess.objects.create(user=self.update, process=process_obj)

    def test_user_stats_process_auth(self):
        self.client.login(username='update', password='update')

        response = self.client.get('/auth/user/stats/', {'stat': 'process'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('processes', data['data'])
        self.assertEqual(len(data['data']['processes']), 10)

    def test_user_stats_process(self):
        response = self.client.get('/auth/user/stats/', {'type': 'process'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_user_stats_files_auth(self):
        self.client.login(username='update', password='update')

        response = self.client.get('/auth/user/stats/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('files', data['data'])
        self.assertEqual(len(data['data']['files']), 10)

    def test_user_stats_files(self):
        response = self.client.get('/auth/user/stats/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_user_details_auth(self):
        self.client.login(username='update', password='update')

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
        self.client.login(username='update', password='update')

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
        self.client.login(username='test', password='test')

        response = self.client.get('/auth/user/regenerate/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Need to generate an api_key, log into either OAuth2 or MyProxyClient')

    def test_regenerate(self):
        response = self.client.get('/auth/user/regenerate/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
