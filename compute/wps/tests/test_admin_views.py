from django import test

from wps import models

class StatsViewTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'files.json']

    def setUp(self):
        self.user = models.User.objects.all()[0]

        self.admin = models.User.objects.all()[1]

        self.admin.is_superuser = True

        self.admin.save()

    def test_stats_processes_authorized(self):
        self.client.login(username=self.admin.username, password=self.admin.username) 

        response = self.client.get('/wps/admin/stats')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('processes', data['data'])
        self.assertEqual(len(data['data']['processes']), 12)

    def test_stats_processes_not_authorized(self):
        self.client.login(username=self.user.username, password=self.user.username) 

        response = self.client.get('/wps/admin/stats')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Forbidden access')

    def test_stats_processes(self):
        response = self.client.get('/wps/admin/stats')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_stats_files_authorized(self):
        self.client.login(username=self.admin.username, password=self.admin.username) 

        response = self.client.get('/wps/admin/stats', {'type': 'files'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('files', data['data'])
        self.assertEqual(len(data['data']['files']), 652)

    def test_stats_files_not_authorized(self):
        self.client.login(username=self.user.username, password=self.user.username) 

        response = self.client.get('/wps/admin/stats', {'type': 'files'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Forbidden access')

    def test_stats_files(self):
        response = self.client.get('/wps/admin/stats', {'type': 'files'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
