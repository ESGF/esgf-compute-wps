from django import test

from wps import models
from wps import views

class APITestCase(test.TestCase):

    def setUp(self):
        self.user = models.User.objects.create_user('test0', 'test0@test.com', '1234')

        models.Auth.objects.create(openid_url='http://test.com/openid/test0', user=self.user)

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

        self.assertEqual(data['status'], 'success')

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
        self.assertEqual(data['error'], 'Failed to authenticate user')

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

    def test_user(self):
        self.client.login(username='test0', password='1234')

        response = self.client.get('/auth/user/')

        data = response.json()

        self.assertEqual(data['status'], 'success')
        
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
