from wps import models

from .common import CommonTestCase

class StatsViewTestCase(CommonTestCase):

    def setUp(self):
        self.user = models.User.objects.create_user('stats', 'stats@gmail.com', 'stats')

        self.admin = models.User.objects.create_user('stats_admin', 'stats@gmail.com', 'stats_admin')

        self.admin.is_superuser = True

        self.admin.save()

    def tearDown(self):
        self.user.delete()

        self.admin.delete()

    def test_stats_processes_authorized(self):
        self.client.login(username='stats_admin', password='stats_admin') 

        response = self.client.get('/wps/stats/processes/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('processes', data['data'])
        self.assertEqual(len(data['data']['processes']), 20)

    def test_stats_processes_not_authorized(self):
        self.client.login(username='stats', password='stats') 

        response = self.client.get('/wps/stats/processes/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Forbidden access')

    def test_stats_processes(self):
        response = self.client.get('/wps/stats/processes/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_stats_files_authorized(self):
        self.client.login(username='stats_admin', password='stats_admin') 

        response = self.client.get('/wps/stats/files/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('files', data['data'])
        self.assertEqual(len(data['data']['files']), 50)

    def test_stats_files_not_authorized(self):
        self.client.login(username='stats', password='stats') 

        response = self.client.get('/wps/stats/files/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Forbidden access')

    def test_stats_files(self):
        response = self.client.get('/wps/stats/files/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
