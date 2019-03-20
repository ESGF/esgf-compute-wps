import datetime
import json
import mock
from django import test
from django.conf import settings

from wps import models
from wps.auth import openid
from wps.views import auth

class AuthViewsTestCase(test.TestCase):
    fixtures = ['users.json']

    def check_failed(self, response):
        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')

        return data

    def check_success(self, response):
        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')

        return data

    def check_redirect(self, response, redirect_count):
        self.assertEqual(response.status_code, 302)

    def test_login_mpc_not_logged_in(self):
        response = self.client.post('/api/mpc/', {})

        self.check_failed(response)

    def test_login_mpc_invalid(self):
        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/api/mpc/', {})

        self.check_failed(response)

    @mock.patch('wps.views.auth.openid.services')
    def test_login_mpc_service_not_supported(self, mock_services):
        user = models.User.objects.all()[0]

        mock_services.side_effect = openid.ServiceError(user.username, 'urn')

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/api/mpc/', {'username': 'test1234', 'password': '1234'})

        self.check_failed(response)

    @mock.patch('wps.views.auth.openid.services')
    def test_login_mpc_parse_failed(self, mock_services):
        mock_services.return_value = [mock.Mock(server_url='http://testbad.com:8181/endpoint')]

        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/api/mpc/', {'username': 'test1234', 'password': '1234'})

        self.check_failed(response)

    @mock.patch('wps.views.auth.MyProxyClient')
    @mock.patch('wps.views.auth.openid.services')
    def test_login_mpc_login_failed(self, mock_services, mock_mpc):
        mock_services.return_value = [mock.Mock(server_url='socket://testbad.com:8181')]

        mock_mpc.return_value = mock.Mock(**{'logon.side_effect': Exception})

        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/api/mpc/', {'username': 'test1234', 'password': '1234'})

        self.check_failed(response)

    @mock.patch('wps.views.auth.MyProxyClient')
    @mock.patch('wps.views.auth.openid.services')
    def test_login_mpc(self, mock_services, mock_mpc):
        mock_services.return_value = [mock.Mock(server_url='socket://testbad.com:8181')]

        mock_mpc.return_value = mock.Mock(**{'logon.return_value': ('test1', 'test2')})

        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/api/mpc/', {'username': 'test1234', 'password': '1234'})

        data = self.check_success(response)['data']

        user.refresh_from_db()

        self.assertEqual(user.auth.cert, 'test1test2')
        self.assertEqual(data['api_key'], user.auth.api_key)
        self.assertEqual(data['type'], 'myproxyclient')

    def test_oauth2_callback_invalid_state(self):
        response = self.client.get('/api/oauth2/callback/', {})

        self.check_failed(response)

    @mock.patch('wps.auth.oauth2.get_certificate')
    @mock.patch('wps.auth.oauth2.get_token')
    @mock.patch('wps.views.auth.openid.services')
    def test_oauth2_callback(self, mock_services, mock_token, mock_certificate):
        mock_services.return_value = (mock.Mock(server_url='http://testbad.com/oauth2/token'), mock.Mock(server_url='http://testbad.com/oauth2/cert'))

        mock_token.return_value = 'new_token'

        mock_certificate.return_value = ('cert', 'key', 'token')

        user = models.User.objects.all()[0]

        user.auth.extra = json.dumps({})

        user.auth.save()

        session = self.client.session

        session['openid'] = user.auth.openid_url

        session['oauth_state'] = {}

        session.save()

        response = self.client.get('/api/oauth2/callback/', {})

        self.check_redirect(response, 1)

        user.auth.refresh_from_db()

    def test_login_oauth2_not_logged_in(self):
        response = self.client.post('/api/oauth2/', {})

        self.check_failed(response)

    @mock.patch('wps.auth.oauth2.get_authorization_url')
    @mock.patch('wps.views.auth.openid.services')
    def test_login_oauth2(self, mock_services, mock_authorization):
        mock_services.return_value = (mock.Mock(), mock.Mock())

        mock_authorization.return_value = ('http://testbad.com/oauth2/redirect', {'test':'test'})

        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/api/oauth2/', {})

        data = self.check_success(response)['data']

        self.assertEqual(data['redirect'], 'http://testbad.com/oauth2/redirect')

        self.assertEqual(self.client.session['openid'], user.auth.openid_url)
        self.assertEqual(self.client.session['oauth_state'], {'test':'test'})

    def test_user_logout_not_logged_in(self):
        response = self.client.get('/api/openid/logout/', {})

        self.check_failed(response)

    def test_user_logout(self):
        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.get('/api/openid/logout/', {})

        self.assertNotIn('_auth_user_id', self.client.session)
        self.check_success(response)

    @mock.patch('wps.views.auth.openid.complete')
    def test_user_login_openid_callback_already_exists(self, mock_openid):
        user = models.User.objects.all()[0]

        mock_openid.return_value = ('http://testbad.com/openid/fake_JnyeGoV7vc', {'email': user.email})

        response = self.client.get('/api/openid/callback/', {})

        self.assertIn('_auth_user_id', self.client.session)
        self.check_redirect(response, 1)

    @mock.patch('wps.views.auth.openid.complete')
    def test_user_login_openid_callback(self, mock_openid):
        mock_openid.return_value = (
            'http://testbad.com/openid/new_user', 
            {
                'email': 'new_user@ankle.com',
                'first': 'hello',
                'last': 'world',
            }
        )

        response = self.client.get('/api/openid/callback/', {})

        self.assertIn('_auth_user_id', self.client.session)
        self.check_redirect(response, 1)

    def test_user_login_openid_invalid(self):
        response = self.client.post('/api/openid/login/', {})

        self.check_failed(response)

    @mock.patch('wps.views.auth.openid.begin')
    def test_user_login_openid(self, mock_openid):
        mock_openid.return_value = 'http://testbad.com/openid'

        response = self.client.post('/api/openid/login/', {'openid_url': 'http://testbad.com/openid/fake_JnyeGoV7vc'})

        data = self.check_success(response)

        self.assertIn('redirect', data['data'])
        self.assertEqual(data['data']['redirect'], 'http://testbad.com/openid')
