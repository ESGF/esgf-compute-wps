from django import test

from wps import models

class ServerViewsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json', 'jobs.json']

    def setUp(self):
        self.user = models.User.objects.all()[0]

    def test_notifications(self):
        response = self.client.get('/wps/notification/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data']['notification'], None)

    def test_processes_auth(self):
        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/wps/processes/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(len(data['data']), 12)

    def test_processes(self):
        response = self.client.get('/wps/processes/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
    
    def test_home(self):
        response = self.client.get('/wps/')

        self.assertEqual(response.status_code, 200)
