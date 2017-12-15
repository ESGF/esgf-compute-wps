import datetime
import json
import mock
from django import test

from wps import models
from wps import settings
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
        self.assertEqual(response.status_code, 200)

        self.assertEqual(len(response.redirect_chain), redirect_count)

    def test_reset_password_missing_parameter(self):
        response = self.client.get('/auth/reset/', {})

        self.check_failed(response)

    def test_reset_password_user_does_not_exist(self):
        response = self.client.get('/auth/reset/', {'token':'unique_token', 'username': 'some_username', 'password': 'new_password'})

        self.check_failed(response)

    def test_reset_password_invalid_state(self):
        user = models.User.objects.all()[0]

        response = self.client.get('/auth/reset/', {'token':'unique_token', 'username': user.username, 'password': 'new_password'})

        self.check_failed(response)

    def test_reset_password_expired_token(self):
        user = models.User.objects.all()[0]

        expire = datetime.datetime.now() - datetime.timedelta(10)

        user.auth.extra = {'reset_token': 'reset_token', 'reset_expire': expire.strftime('%x %X')}

        user.auth.save()

        response = self.client.get('/auth/reset/', {'token':'unique_token', 'username': user.username, 'password': 'new_password'})

        self.check_failed(response)

    def test_reset_password_mismatch_token(self):
        user = models.User.objects.all()[0]

        expire = datetime.datetime.now() + datetime.timedelta(10)

        user.auth.extra = json.dumps({'reset_token': 'bad_token', 'reset_expire': expire.strftime('%x %X')})

        user.auth.save()

        response = self.client.get('/auth/reset/', {'token':'not_the_right_token', 'username': user.username, 'password': 'new_password'})

        self.check_failed(response)

    def test_reset_password(self):
        user = models.User.objects.all()[0]

        expire = datetime.datetime.now() + datetime.timedelta(10)

        user.auth.extra = json.dumps({'reset_token': 'unique_token', 'reset_expire': expire.strftime('%x %X')})

        user.auth.save()

        response = self.client.get('/auth/reset/', {'token': 'unique_token', 'username': user.username, 'password': 'new_password'})

        data = self.check_success(response)['data']

        self.assertIn('redirect', data)
        self.assertEqual(data['redirect'], settings.LOGIN_URL)

        user.auth.refresh_from_db()

        extra = json.loads(user.auth.extra)

        self.assertNotIn('reset_token', extra)
        self.assertNotIn('reset_expire', extra)

    def test_forgot_password_missing_parameter(self):
        response = self.client.get('/auth/forgot/password/', {})

        self.check_failed(response)

    def test_forgot_password_user_does_not_exist(self):
        response = self.client.get('/auth/forgot/password/', {'username': 'test1234'})

        self.check_failed(response)

    @mock.patch('wps.views.auth.send_mail')
    def test_forgot_password(self, mock_send_mail):
        user = models.User.objects.all()[0]

        response = self.client.get('/auth/forgot/password/', {'username': user.username})

        data = self.check_success(response)['data']

        self.assertIn('redirect', data)

        user.refresh_from_db()

        extra = json.loads(user.auth.extra)
        
        self.assertIn('reset_token', extra)
        self.assertIn('reset_expire', extra)

        mock_send_mail.assert_called()

    def test_forgot_username_missing_parameter(self):
        response = self.client.get('/auth/forgot/username/', {})

        self.check_failed(response)

    def test_forgot_username_user_does_not_exist(self):
        response = self.client.get('/auth/forgot/username/', {'email': 'new_user@test.com'})

        self.check_failed(response)

    @mock.patch('wps.views.auth.send_mail')
    def test_forgot_username(self, mock_send_mail):
        user = models.User.objects.all()[0]

        response = self.client.get('/auth/forgot/username/', {'email': user.email})

        data = self.check_success(response)['data']

        mock_send_mail.assert_called()

        self.assertEqual(data['redirect'], settings.LOGIN_URL)

    def test_login_mpc_not_logged_in(self):
        response = self.client.post('/auth/login/mpc/', {})

        self.check_failed(response)

    def test_login_mpc_invalid(self):
        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/auth/login/mpc/', {})

        self.check_failed(response)

    @mock.patch('wps.views.auth.openid_services')
    def test_login_mpc_service_not_supported(self, mock_services):
        mock_services.return_value = []

        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/auth/login/mpc/', {'username': 'test1234', 'password': '1234'})

        self.check_failed(response)

    @mock.patch('wps.views.auth.openid_services')
    def test_login_mpc_parse_failed(self, mock_services):
        mock_services.return_value = [mock.Mock(server_url='http://test.com:8181/endpoint')]

        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/auth/login/mpc/', {'username': 'test1234', 'password': '1234'})

        self.check_failed(response)

    @mock.patch('wps.views.auth.MyProxyClient')
    @mock.patch('wps.views.auth.openid_services')
    def test_login_mpc_login_failed(self, mock_services, mock_mpc):
        mock_services.return_value = [mock.Mock(server_url='socket://test.com:8181')]

        mock_mpc.return_value = mock.Mock(**{'logon.side_effect': Exception})

        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/auth/login/mpc/', {'username': 'test1234', 'password': '1234'})

        self.check_failed(response)

    @mock.patch('wps.views.auth.MyProxyClient')
    @mock.patch('wps.views.auth.openid_services')
    def test_login_mpc(self, mock_services, mock_mpc):
        mock_services.return_value = [mock.Mock(server_url='socket://test.com:8181')]

        mock_mpc.return_value = mock.Mock(**{'logon.return_value': ('test1', 'test2')})

        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/auth/login/mpc/', {'username': 'test1234', 'password': '1234'})

        data = self.check_success(response)['data']

        user.refresh_from_db()

        self.assertEqual(user.auth.cert, 'test1test2')
        self.assertEqual(data['api_key'], user.auth.api_key)
        self.assertEqual(data['type'], 'myproxyclient')

    def test_oauth2_callback_invalid_state(self):
        response = self.client.get('/auth/callback/', {}, follow=True)

        self.check_redirect(response, 1)

    @mock.patch('wps.auth.oauth2.get_certificate')
    @mock.patch('wps.auth.oauth2.get_token')
    @mock.patch('wps.views.auth.openid_services')
    def test_oauth2_callback(self, mock_services, mock_token, mock_certificate):
        mock_services.return_value = (mock.Mock(server_url='http://test.com/oauth2/token'), mock.Mock(server_url='http://test.com/oauth2/cert'))

        mock_token.return_value = 'new_token'

        mock_certificate.return_value = ('cert', 'key', 'token')

        user = models.User.objects.all()[0]

        user.auth.extra = json.dumps({})

        user.auth.save()

        session = self.client.session

        session['openid'] = user.auth.openid_url

        session['oauth_state'] = {}

        session.save()

        response = self.client.get('/auth/callback/', {}, follow=True)

        self.check_redirect(response, 1)

        user.auth.refresh_from_db()

        print 'FUCKKKKKKK', user.auth.extra

    def test_login_oauth2_not_logged_in(self):
        response = self.client.post('/auth/login/oauth2/', {})

        self.check_failed(response)

    @mock.patch('wps.auth.oauth2.get_authorization_url')
    @mock.patch('wps.views.auth.openid_services')
    def test_login_oauth2(self, mock_services, mock_authorization):
        mock_services.return_value = (mock.Mock(), mock.Mock())

        mock_authorization.return_value = ('http://test.com/oauth2/redirect', {'test':'test'})

        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.post('/auth/login/oauth2/', {})

        data = self.check_success(response)['data']

        self.assertEqual(data['redirect'], 'http://test.com/oauth2/redirect')

        self.assertEqual(self.client.session['openid'], user.auth.openid_url)
        self.assertEqual(self.client.session['oauth_state'], {'test':'test'})

    def test_user_logout_not_logged_in(self):
        response = self.client.get('/auth/logout/', {})

        self.check_failed(response)

    def test_user_logout(self):
        user = models.User.objects.all()[0]

        self.client.login(username=user.username, password=user.username)

        response = self.client.get('/auth/logout/', {})

        self.assertNotIn('_auth_user_id', self.client.session)
        self.check_success(response)

    def test_user_login_doesnt_exist(self):
        response = self.client.post('/auth/login/', {'username': 'test1234', 'password': '1234'})

        self.check_failed(response)

    def test_user_login_invalid(self):
        response = self.client.post('/auth/login/', {})

        self.check_failed(response)

    def test_user_login(self):
        user = models.User.objects.all()[0]

        response = self.client.post('/auth/login/', {'username': user.username, 'password': user.username})

        self.assertIn('_auth_user_id', self.client.session)

        data = self.check_success(response)['data']

        self.assertEqual(data['username'], user.username)
        self.assertEqual(data['openid'], user.auth.openid_url)
        self.assertFalse(data['admin'])
        self.assertTrue(data['local_init'])
        self.assertEqual(data['api_key'], '')
        self.assertEqual(data['type'], '')
        self.assertEqual(data['email'], user.email)

    @mock.patch('wps.views.auth.openid_complete')
    def test_user_login_openid_callback_already_exists(self, mock_openid):
        user = models.User.objects.all()[0]

        mock_openid.return_value = ('http://test.com/openid', {'email': user.email})

        response = self.client.get('/auth/callback/openid', {}, follow=True)

        self.assertIn('_auth_user_id', self.client.session)
        self.check_redirect(response, 2)

    @mock.patch('wps.views.auth.openid_complete')
    def test_user_login_openid_callback(self, mock_openid):
        mock_openid.return_value = ('http://test.com/openid', {'email': 'http://test.com/openid/new_user'})

        response = self.client.get('/auth/callback/openid', {}, follow=True)

        self.assertIn('_auth_user_id', self.client.session)
        self.check_redirect(response, 2)

    def test_user_login_openid_invalid(self):
        response = self.client.post('/auth/login/openid/', {})

        self.check_failed(response)

    @mock.patch('wps.views.auth.openid_begin')
    def test_user_login_openid(self, mock_openid):
        mock_openid.return_value = 'http://test.com/openid'

        response = self.client.post('/auth/login/openid/', {'openid_url': 'http://test.com/openid'})

        data = self.check_success(response)

        self.assertIn('redirect', data['data'])
        self.assertEqual(data['data']['redirect'], 'http://test.com/openid')

    def test_create_already_exists(self):
        user = models.User.objects.all()[0]

        user_data = {
            'username': user.username,
            'email': user.email,
            'openid': user.auth.openid,
            'password': 'abcd'
        }

        response = self.client.post('/auth/create/', user_data)

        self.check_failed(response)

    def test_create_invalid(self):
        response = self.client.post('/auth/create/', {})

        self.check_failed(response)

    def test_create_user_already_exists(self):
        user = models.User.objects.all()[0]

        user_data = {
            'username': user.username,
            'email': user.email,
            'openid': user.auth.openid_url,
            'password': 'new_password'
        }

        response = self.client.post('/auth/create/', user_data)

        self.check_failed(response)

    @mock.patch('wps.views.auth.send_mail')
    def test_create_send_mail_failed(self, mock_send):
        mock_send.side_effect = Exception

        user = {
            'username': 'test_user',
            'email': 'test_user@test.com',
            'openid': 'http://test.com/openid/test_user',
            'password': 'abcd'
        }

        response = self.client.post('/auth/create/', user)

        self.check_success(response)

        mock_send.assert_called()

    @mock.patch('wps.views.auth.send_mail')
    def test_create(self, mock_send):
        user = {
            'username': 'test_user',
            'email': 'test_user@test.com',
            'openid': 'http://test.com/openid/test_user',
            'password': 'abcd'
        }

        response = self.client.post('/auth/create/', user)

        self.check_success(response)

        mock_send.assert_called()

class AuthFunctionTestCase(test.TestCase):
    fixtures = ['users.json']
    mock_services = [
        mock.Mock(type_uris=['test_uri_1']),
        mock.Mock(type_uris=['test_uri_2']),
        mock.Mock(type_uris=['test_uri_3']),
    ]

    @mock.patch('wps.views.auth.ax.FetchResponse.fromSuccessResponse')
    def test_handle_openid_attribute_exchange_none(self, mock_fetch):
        mock_fetch.return_value = None

        attrs = auth.handle_openid_attribute_exchange(mock.Mock())

        self.assertEqual(attrs, {'email': None})

    @mock.patch('wps.views.auth.ax.FetchResponse.fromSuccessResponse')
    def test_handle_openid_attribute_exchange(self, mock_fetch):
        mock_fetch.return_value = mock.Mock(**{'get.return_value': ['test@test.com']})

        attrs = auth.handle_openid_attribute_exchange(mock.Mock())

        self.assertEqual(attrs['email'], 'test@test.com')

    def test_openid_complete_exception(self):
        with self.assertRaises(Exception):
            auth.openid_complete(mock.Mock())

    @mock.patch('wps.views.auth.consumer.Consumer')
    def test_openid_complete_failure(self, mock_consumer):
        mock_consumer.return_value = mock.Mock(**{'complete.return_value': mock.Mock(status='failure')})

        with self.assertRaises(Exception):
            auth.openid_complete(mock.Mock())

    @mock.patch('wps.views.auth.consumer.Consumer')
    def test_openid_complete_cancel(self, mock_consumer):
        mock_consumer.return_value = mock.Mock(**{'complete.return_value': mock.Mock(status='cancel')})

        with self.assertRaises(Exception):
            auth.openid_complete(mock.Mock())

    @mock.patch('wps.views.auth.handle_openid_attribute_exchange')
    @mock.patch('wps.views.auth.consumer.Consumer')
    def test_openid_complete(self, mock_consumer, mock_attr):
        mock_attr.return_value = {'email': 'test@test.com'}

        mock_complete = mock.Mock(**{'getDisplayIdentifier.return_value': 'http://test.com/openid/test'})

        mock_consumer.return_value = mock.Mock(**{'complete.return_value': mock_complete})

        url, attrs = auth.openid_complete(mock.Mock())

        self.assertEqual(url, 'http://test.com/openid/test')
        self.assertEqual(attrs, mock_attr.return_value)

    def test_openid_begin_exception(self):
        with self.assertRaises(Exception):
            auth.openid_begin(mock.Mock(), 'http://test.com/openid')

    @mock.patch('wps.views.auth.consumer.Consumer')
    def test_openid_begin(self, mock_consumer):
        mock_begin = mock.Mock(**{'redirectURL.return_value': 'http://test.com/openid/begin'})
        mock_consumer.return_value = mock.Mock(**{'begin.return_value': mock_begin})

        url = auth.openid_begin(mock.Mock(session={}), 'http://test.com/openid')

        self.assertEqual(url, 'http://test.com/openid/begin')

        mock_consumer.assert_called_with({}, mock_consumer.call_args[0][1])
        mock_consumer.return_value.begin.assert_called_with('http://test.com/openid')

        mock_begin.redirectURL.assert_called_with(settings.OPENID_TRUST_ROOT, settings.OPENID_RETURN_TO)

    def test_openid_services_discovery_error(self):
        with self.assertRaises(Exception):
            auth.openid_services('http://test.com/openid', ['urn.test1', 'urn.test2'])

    @mock.patch('wps.views.auth.discover.discoverYadis')
    def test_openid_services_service_not_supported(self, mock_discover):
        mock_discover.return_value = ('http://test.com/openid', self.mock_services)

        with self.assertRaises(Exception):
            auth.openid_services('http://test.com/openid', ['urn.test1', 'urn.test2'])

    @mock.patch('wps.views.auth.discover.discoverYadis')
    def test_openid_services(self, mock_discover):
        mock_discover.return_value = ('http://test.com/openid', self.mock_services)

        services = auth.openid_services('http://test.com/openid', ['test_uri_1', 'test_uri_3'])

        self.assertEqual(len(services), 2)

    def test_openid_find_service_by_type_not_found(self):
        service = auth.openid_find_service_by_type(self.mock_services, 'test_uri_10')

        self.assertIsNone(service)

    def test_openid_find_service_by_type(self):
        service = auth.openid_find_service_by_type(self.mock_services, 'test_uri_1')

        self.assertIsNotNone(service)

    def test_update_user(self):
        user = models.User.objects.all()[0]

        auth.update_user(user, 'myproxyclient', 'certs', test='new attribute') 

        self.assertEqual(user.auth.type, 'myproxyclient')
        self.assertEqual(user.auth.cert, 'certs')
        self.assertEqual(user.auth.extra, '{"test": "new attribute"}')
