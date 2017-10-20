from django import test

from .common import CommonTestCase

class ServerViewsTestCase(CommonTestCase):

    def test_notifications(self):
        response = self.client.get('/wps/notification/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data']['notification'], None)

    def test_processes_auth(self):
        self.client.login(username='test', password='test')

        response = self.client.get('/wps/processes/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(len(data['data']), 20)

    def test_processes(self):
        response = self.client.get('/wps/processes/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
    
    def test_home(self):
        response = self.client.get('/wps/')

        self.assertEqual(response.status_code, 200)
