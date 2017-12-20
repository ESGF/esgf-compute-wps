from django import test

from . import helpers
from wps import models

class ServerViewsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json', 'jobs.json']

    def setUp(self):
        self.user = models.User.objects.all()[0]

    def test_processes_auth(self):
        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/wps/processes/')

        data = helpers.check_success(self, response)

        self.assertEqual(len(data['data']), 12)

    def test_processes(self):
        response = self.client.get('/wps/processes/')

        helpers.check_failed(self, response)
    
    def test_home(self):
        response = self.client.get('/wps/')

        self.assertEqual(response.status_code, 200)
