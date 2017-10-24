import datetime
import json

import mock

from .common import CommonTestCase
from wps import models
from wps import settings

from . import common

class AuthViewsTestCase(CommonTestCase):

    def setUp(self):
        self.auth_user = models.User.objects.create_user('auth_user', 'auth_user@gmail.com', 'auth_user')

        models.Auth.objects.create(user=self.auth_user,
                                   openid_url='http://doesnotexist.com/openid/doesnotexist',
                                   extra={})

    def tearDonw(self):
        self.auth_user.delete()

    def test_create_complete(self):
        params = {
            'username': 'create',
            'openid': 'http://doesnotexist.com/doesnotexist',
            'password': 'create',
            'email': 'doesnotexist@hello.com'
        }

        response = self.client.post('/auth/create/', params)

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data'], 'Successfully created account for "create"')

        self.assertTrue(self.client.login(username='create', password='create'))

    def test_create(self):
        response = self.client.post('/auth/create/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], {u'username': [u'This field is required.'], u'openid': [u'This field is required.'], u'password': [u'This field is required.'], u'email': [u'This field is required.']})

    def test_user_login_openid_auth(self):
        response = self.client.post('/auth/login/openid/', {'openid_url': 'http://doesnotexist.com/openid/doesnotexist'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'OpenID discovery error')

    def test_user_login_openid(self):
        response = self.client.post('/auth/login/openid/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertIn('openid_url', data['error'])

    def test_user_login_openid_callback(self):
        response = self.client.get('/auth/callback/openid/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'OpenID authentication failed')

    def test_logout_auth(self):
        self.client.login(username='auth_user', password='auth_user')

        response = self.client.get('/auth/logout/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data'], 'Logged out')

    def test_logout(self):
        response = self.client.get('/auth/logout/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_login_auth(self):
        response = self.client.post('/auth/login/', {'username': 'auth_user', 'password': 'auth_user'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')

        expected = ('username', 'openid', 'admin', 'local_init', 'api_key', 'type', 'email')

        for exp in expected:
            self.assertIn(exp, data['data'])

    def test_login(self):
        response = self.client.post('/auth/login/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertIn('username', data['error'])
        self.assertIn('password', data['error'])

    def test_oauth2_callback_session(self):
        self.client.session['openid'] = 'http://doesnotexist.com/openid/doesnotexist'

        self.client.session['oauth_state'] = {}

        response = self.client.get('/auth/callback/')

        self.assertEqual(response.status_code, 302)
        
    def test_oauth2_callback(self):
        response = self.client.get('/auth/callback/')

        self.assertEqual(response.status_code, 302)

    def test_login_mpc_credentials(self):
        self.client.login(username='auth_user', password='auth_user')

        response = self.client.post('/auth/login/mpc/', {'username': 'test', 'password': 'test'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Failed to retrieve OpenID')

    def test_login_mpc_auth(self):
        self.client.login(username='auth_user', password='auth_user')

        response = self.client.post('/auth/login/mpc/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertIn('username', data['error'])
        self.assertIn('password', data['error'])

    def test_login_mpc(self):
        response = self.client.post('/auth/login/mpc/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_login_oauth2_auth(self):
        self.client.login(username='auth_user', password='auth_user')

        response = self.client.post('/auth/login/oauth2/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Failed to retrieve OpenID')

    def test_login_oauth2(self):
        response = self.client.post('/auth/login/oauth2/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_forgot_password_invalid_username(self):
        response = self.client.get('/auth/forgot/password/', {'username': 'doesnotexist'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Username "doesnotexist" does not exist')

    @mock.patch('wps.views.auth.send_mail')
    def test_forgot_password_username(self, send_mail_mock):
        send_mail_mock.side_effect = Exception()

        response = self.client.get('/auth/forgot/password/', {'username': 'auth_user'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Error sending reset password email')

    def test_forgot_pasword(self):
        response = self.client.get('/auth/forgot/password/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Please provide the username you are trying to reset the password for')

    def test_forgot_username_invalid_email(self):
        response = self.client.get('/auth/forgot/username/', {'email': 'doesntexist@gmail.com'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'There is not user associated with the email "doesntexist@gmail.com"')

    @mock.patch('wps.views.auth.send_mail')
    def test_forgot_username_email(self, send_mail_mock):
        send_mail_mock.side_effect = Exception()

        response = self.client.get('/auth/forgot/username/', {'email': 'auth_user@gmail.com'})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Failed sending reset link to accounts email')

    def test_forgot_username(self):
        response = self.client.get('/auth/forgot/username/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Please provide the email address of the account')

    def test_reset_password_expired(self):
        token = common.random_str(10)

        extra = {
            'reset_token': token,
            'reset_expire': datetime.datetime.now()-datetime.timedelta(10)
        }

        self.auth_user.auth.extra = json.dumps(extra, default=lambda x: x.strftime('%x %X'))

        self.auth_user.auth.save()

        params = {
            'token': token,
            'username': 'auth_user',
            'password': 'new_password'
        }

        response = self.client.get('/auth/reset/', params)

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Reset token has expired')

    def test_reset_password_params(self):
        token = common.random_str(10)

        extra = {
            'reset_token': token,
            'reset_expire': datetime.datetime.now()+datetime.timedelta(10)
        }

        self.auth_user.auth.extra = json.dumps(extra, default=lambda x: x.strftime('%x %X'))

        self.auth_user.auth.save()

        params = {
            'token': token,
            'username': 'auth_user',
            'password': 'new_password'
        }

        response = self.client.get('/auth/reset/', params)

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertIn('redirect', data['data'])
        self.assertEqual(data['data']['redirect'], settings.LOGIN_URL)

    def test_reset_password(self):
        response = self.client.get('/auth/reset/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Missing required parameter "\'token\'"')
