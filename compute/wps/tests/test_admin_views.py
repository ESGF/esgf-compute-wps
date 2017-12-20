from django import test

from . import helpers
from wps import models

class StatsViewTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'files.json']

    def setUp(self):
        self.user = models.User.objects.all()[0]

        self.admin = models.User.objects.all()[1]

        self.admin.is_superuser = True

        self.admin.save()

    def test_stats_processes(self):
        self.client.login(username=self.admin.username, password=self.admin.username) 

        response = self.client.get('/wps/admin/stats')

        data = helpers.check_success(self, response)

        self.assertIn('processes', data['data'])
        self.assertEqual(len(data['data']['processes']), 12)

    def test_stats_processes_missing_authorization(self):
        self.client.login(username=self.user.username, password=self.user.username) 

        response = self.client.get('/wps/admin/stats')

        helpers.check_failed(self, response)

    def test_stats_processes_missing_authentication(self):
        response = self.client.get('/wps/admin/stats')

        helpers.check_failed(self, response)

    def test_stats_files(self):
        self.client.login(username=self.admin.username, password=self.admin.username) 

        response = self.client.get('/wps/admin/stats', {'type': 'files'})

        data = helpers.check_success(self, response)

        self.assertIn('files', data['data'])
        self.assertEqual(len(data['data']['files']), 652)

    def test_stats_files_missing_authorization(self):
        self.client.login(username=self.user.username, password=self.user.username) 

        response = self.client.get('/wps/admin/stats', {'type': 'files'})

        helpers.check_failed(self, response)

    def test_stats_files_missing_authentication(self):
        response = self.client.get('/wps/admin/stats', {'type': 'files'})

        helpers.check_failed(self, response)
