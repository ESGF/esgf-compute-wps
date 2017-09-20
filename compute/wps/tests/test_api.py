import json

import mock
from django import test

from wps import models
from wps import views

class APITestCase(test.TestCase):

    def setUp(self):
        self.admin = models.User.objects.create_superuser('admin0', 'admin0@test.com', '1234')

        models.Auth.objects.create(openid_url='http://test.com/openid/admin0', user=self.admin)

        self.user = models.User.objects.create_user('test0', 'test0@test.com', '1234')

        models.Auth.objects.create(openid_url='http://test.com/openid/test0', user=self.user)

        server = models.Server.objects.create(host='default')

        process = server.processes.create(identifier='CDAT.subset', backend='local')

        self.job = models.Job.objects.create(server=server, user=self.user, process=process)

        self.job.accepted()

        self.job.started()

        self.job.succeeded()

    @mock.patch('wps.views.consumer.Consumer')
    def test_user_login_openid_callback_failure(self, consumer):
        complete_attrs = {'status': 'failure'}

        complete = mock.Mock(**complete_attrs)

        consumer_attrs = {'complete.return_value': complete}  

        consumer.return_value = mock.Mock(**consumer_attrs)

        response = self.client.get('/auth/callback/openid/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertRegexpMatches(data['error'], 'OpenID authentication failed: .*')

    @mock.patch('wps.views.consumer.Consumer')
    def test_user_login_openid_callback_cancelled(self, consumer):
        complete_attrs = {'status': 'cancel'}

        complete = mock.Mock(**complete_attrs)

        consumer_attrs = {'complete.return_value': complete}  

        consumer.return_value = mock.Mock(**consumer_attrs)

        response = self.client.get('/auth/callback/openid/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'OpenID authentication cancelled')

    @mock.patch('wps.views.consumer.Consumer')
    def test_user_login_openid_callback(self, consumer):
        complete_attrs = {'getDisplayIdentifier.return_value': 'openid_url'}

        complete = mock.Mock(**complete_attrs)

        consumer_attrs = {'complete.return_value': complete}  

        consumer.return_value = mock.Mock(**consumer_attrs)

        with mock.patch('wps.views.__handle_openid_attribute_exchange') as handle:
            handle.return_value = {'email': 'test@home.gov'}

            with self.assertNumQueries(11):
                response = self.client.get('/auth/callback/openid/')

        self.assertTrue('http://0.0.0.0:8000/wps/home/login/callback?expires' in response.url)

    def test_user_login_openid_discovery_error(self):
        response = self.client.post('/auth/login/openid/', {'openid_url': 'http://wps.gov/openid/username'})

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'OpenID discovery error')

    @mock.patch('wps.views.consumer.Consumer')
    def test_user_login_openid(self, consumer):
        auth_request_attrs = {'redirectURL.return_value': 'redirect'}

        auth_request = mock.Mock(**auth_request_attrs)

        consumer_attrs = {'begin.return_value': auth_request}

        consumer.return_value = mock.Mock(**consumer_attrs)

        response = self.client.post('/auth/login/openid/', {'openid_url': 'http://wps.gov/openid/username'})

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertDictEqual(data['data'], {'redirect': 'redirect'})

    def test_job_remove_all_not_logged_in(self):
        response = self.client.get('/wps/jobs/remove/')

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be logged in to remove jobs')

    def test_job_remove_all(self):
        self.client.login(username='test0', password='1234')

        response = self.client.get('/wps/jobs/remove/')

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data'], u'Removed all jobs')

    def test_job_remove_not_owner(self):
        self.client.login(username='admin0', password='1234')

        response = self.client.get('/wps/jobs/{}/remove/'.format(self.job.pk))

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be the owner of the job to remove')

    def test_job_remove_not_logged_in(self):
        response = self.client.get('/wps/jobs/{}/remove/'.format(self.job.pk))

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be logged in to remove a job')

    def test_job_remove(self):
        self.client.login(username='test0', password='1234')

        response = self.client.get('/wps/jobs/{}/remove/'.format(self.job.pk))

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertDictEqual(data['data'], {u'job': unicode(self.job.pk)})
        
    def test_job_not_logged_in(self):
        response = self.client.get('/wps/jobs/{}/'.format(self.job.pk))

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be logged in to view job details')

    def test_job(self):
        self.client.login(username='test0', password='1234')

        response = self.client.get('/wps/jobs/{}/'.format(self.job.pk))

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')

    def test_jobs_not_logged_in(self):
        response = self.client.get('/wps/jobs/')

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be logged in to view job history')

    def test_jobs(self):
        self.client.login(username='test0', password='1234')

        response = self.client.get('/wps/jobs/')

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')

    def test_generate_not_logged_in(self):
        query = {
            'process': 'CDAT.subset',
            'variable': 'tas',
            'files': 'file:///test.nc',
            'regrid': 'None'
        }

        response = self.client.post('/wps/generate/', query)

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')

    def test_generate(self):
        query = {
            'process': 'CDAT.subset',
            'variable': 'tas',
            'files': 'file:///test.nc',
            'regrid': 'None'
        }

        self.client.login(username='test0', password='1234')

        response = self.client.post('/wps/generate/', query)

        self.assertEqual(response.status_code, 200)

    def test_execute_not_logged_in(self):
        query = {
            'process': 'CDAT.subset',
            'variable': 'tas',
            'files': 'file:///test.nc',
            'regrid': 'None'
        }

        response = self.client.post('/wps/execute/', query)

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be logged in to execute a job')

    def test_execute(self):
        query = {
            'process': 'CDAT.subset',
            'variable': 'tas',
            'files': 'file:///test.nc',
            'regrid': 'None'
        }

        self.client.login(username='test0', password='1234')

        response = self.client.post('/wps/execute/', query)

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'success')
        self.assertTrue('report' in data['data'])

    def test_search_not_logged_in(self):
        response = self.client.get('/wps/search/')

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')

    def test_search(self):
        self.client.login(username='test0', password='1234')

        response = self.client.get('/wps/search/')

        data = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'failed')

    def test_wps(self):
        response = self.client.get('/wps/')

        self.assertEqual(response.status_code, 200)

    def test_mpc_not_logged_in(self):
        query = {
            'username': 'test0',
            'password': '4321'
        }

        response = self.client.post('/auth/login/mpc/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')

    def test_mpc(self):
        query = {
            'username': 'test0',
            'password': '4321'
        }

        self.client.login(username='test0', password='1234')

        response = self.client.post('/auth/login/mpc/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')

    def test_oauth2_not_logged_in(self):
        response = self.client.post('/auth/login/oauth2/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be logged in to authenticate using ESGF OAuth2')

    def test_oauth2(self):
        self.client.login(username='test0', password='1234')

        response = self.client.post('/auth/login/oauth2/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')

    def test_logout_not_logged_in(self):
        response = self.client.get('/auth/logout/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')

    def test_logout(self):
        self.client.login(username='test0', password='1234')

        response = self.client.get('/auth/logout/')

        data = response.json()

        self.assertEqual(data['status'], 'success')

    def test_login_bad_login(self):
        query = {
            'username': 'test0',
            'password': '4321'
        }

        response = self.client.post('/auth/login/', query)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Authentication failed')

    def test_login(self):
        query = {
            'username': 'test0',
            'password': '1234'
        }

        response = self.client.post('/auth/login/', query)

        data = response.json()

        self.assertEqual(data['status'], 'success')

    def test_regenerate_not_logged_in(self):
        response = self.client.get('/auth/user/regenerate/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be logged in to regenerate api key')

    def test_regenerate(self):
        self.client.login(username='test0', password='1234')

        response = self.client.get('/auth/user/regenerate/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Need to generate an api_key, log into either OAuth2 or MyProxyClient')

    def test_user_not_logged_in(self):
        response = self.client.get('/auth/user/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be logged in to retieve user details')

    def test_user_admin(self):
        self.client.login(username='admin0', password='1234')

        response = self.client.get('/auth/user/')

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data']['admin'], True)

    def test_user(self):
        self.client.login(username='test0', password='1234')

        response = self.client.get('/auth/user/')

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data']['admin'], False)
        
    def test_update_not_logged_in(self):
        query = {
            'email': 'test0@new.com',
            'openid': 'http://test1.com/openid/test0',
            'password': '4321'
        }

        response = self.client.post('/auth/update/', query)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Must be logged in to update account')

    def test_update(self):
        self.client.login(username='test0', password='1234')

        query = {
            'email': 'test0@new.com',
            'openid': 'http://test1.com/openid/test0',
            'password': '4321'
        }

        response = self.client.post('/auth/update/', query)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        
        user = data['data']

        self.assertEqual(user['email'], 'test0@new.com')
        self.assertEqual(user['openid'], 'http://test1.com/openid/test0')

        self.client.logout()

        self.assertTrue(self.client.login(username='test0', password='4321'))

    def test_create_missing_fields(self):
        response = self.client.post('/auth/create/')

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'].keys(), ['username', 'openid', 'password', 'email'])

    def test_create(self):
        query = {
            'username': 'test1',
            'openid': 'http://openid/test1',
            'password': 'changeit',
            'email': 'test1@test.com'
        }

        response = self.client.post('/auth/create/', query)

        data = response.json()

        self.assertEqual(data['status'], 'success')
