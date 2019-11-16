#! /usr/bin/env python

import os

import mock
import time
from subprocess import PIPE, Popen

from django import test
from django.test import override_settings
from compute_wps import models
from compute_wps.auth import oauth2
from . import helpers
from OpenSSL import crypto
from requests_oauthlib import TokenUpdated
from oauthlib.oauth2.rfc6749.errors import InvalidGrantError

class TestOAuth2(test.TestCase):
    fixtures = ['users.json']
    test_token = {
        "token_type": "Bearer",
        "access_token": "asdfoiw37850234lkjsdfsdf",
        "refresh_token": "refreshed_token",
        "expires_in": 3600,
        "expires_at": time.time() + 3600,
        }
    openid_url = 'http://test.com/openid'
    certs = ['cert1', 'cert2']
    api_key = 'updated_api_key'
    type = 'test_auth_type'

    token_url = "https://token_url"
    certificate_url = "https://certificate_url"
    authorization_url = "https://authorization_url"

    user_name = "test_user1"

    def _create_user_with_auth(self, auth_type):
        test_user = models.User.objects.create_user(self.user_name,
                                                    'test_email@test.com', 'test_password1')
        auth = models.Auth.objects.create(openid_url=self.openid_url, user=test_user)
        auth.update(auth_type, self.certs, self.api_key, token=self.test_token, state='good_state')
        test_token, test_state = test_user.auth.get('token', 'state')
        return auth, test_user

    def setUp(self):
        os.environ['OAUTH2_CLIENT_ID'] = ''
        os.environ['OAUTH2_CLIENT_SECRET'] = ''
        os.environ['SECRET_KEY'] = 'some_secret_key'
        os.environ['PROVISIONER_FRONTEND'] = '127.0.0.1'
        self.user = models.User.objects.first()

    @mock.patch('compute_wps.auth.oauth2.crypto')
    def test_check_certificate_exception(self, crypto_mock):
        crypto_mock.load_certificate = mock.Mock(side_effect=Exception)

        with self.assertRaises(oauth2.CertificateError):
            oauth2.check_certificate(self.user)

    @mock.patch('compute_wps.auth.oauth2.crypto')
    def test_check_certificate_now_after_after(self, crypto_mock):
        crypto_mock.load_certificate.return_value = helpers.generate_certificate(0, -1)
        ret_val = oauth2.check_certificate(self.user)
        self.assertFalse(ret_val)

    @mock.patch('compute_wps.auth.oauth2.crypto')
    def test_check_certificate_now_before_before(self, crypto_mock):
        crypto_mock.load_certificate.return_value = helpers.generate_certificate(2)
        ret_val = oauth2.check_certificate(self.user)
        self.assertFalse(ret_val)

    @mock.patch('compute_wps.auth.oauth2.crypto')
    def test_check_certificate(self, crypto_mock):
        cert = helpers.generate_certificate()
        crypto_mock.load_certificate.return_value = cert

        ret_val = oauth2.check_certificate(self.user)
        self.assertTrue(ret_val)

    def test_refresh_certificate_myproxyclient(self):
        auth, user = self._create_user_with_auth('myproxyclient')
        cert_err_msg = "Certificate error for user"
        myproxy_msg = "MyProxyClient certificate has expired"
        msg = "{c} \"{u}\": {m}".format(c=cert_err_msg,
                                        u=self.user_name,
                                        m=myproxy_msg)

        with self.assertRaises(oauth2.CertificateError) as e:
            certs = oauth2.refresh_certificate(user, self.token_url, self.certificate_url)
        self.assertEqual(msg, str(e.exception))

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    @mock.patch('compute_wps.auth.oauth2.check_certificate')
    def test_refresh_certificate(self, mock_check, mock_oauth2):
        auth, test_user = self._create_user_with_auth(self.type)
        test_token, test_state = test_user.auth.get('token', 'state')
        status_code = 200
        self._setup_mock_status_code(mock_check, mock_oauth2, status_code)
        cert = oauth2.refresh_certificate(test_user, self.token_url, self.certificate_url)
        self.assertEqual(cert, auth.cert)

    def test_generate_certificate_request(self):
        private_key, cert_request = oauth2.generate_certificate_request()
        # not sure how to check the cert_request
        self.assertTrue("BEGIN PRIVATE KEY" in private_key)

    @mock.patch('compute_wps.auth.oauth2.check_certificate')
    def test_get_certificate_good_cert_already(self, mock_check):
        mock_check.return_value = True
        cert = oauth2.get_certificate(self.user, self.token_url, self.certificate_url)
        self.assertEqual(cert, self.user.auth.cert)

    def _setup_mock_status_code(self, mock_check, mock_oauth2, status_code):
        test_response_text = 'post response text'
        mock_check.return_value = False
        mock_session = mock.MagicMock()
        mock_post_response = mock.MagicMock(status_code=status_code, text=test_response_text)
        mock_session.post.return_value = mock_post_response
        mock_oauth2.return_value = mock_session

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    @mock.patch('compute_wps.auth.oauth2.check_certificate')
    def test_get_certificate(self, mock_check, mock_oauth2):
        status_code = 200
        auth, test_user = self._create_user_with_auth(self.type)
        self._setup_mock_status_code(mock_check, mock_oauth2, status_code)
        cert = oauth2.get_certificate(test_user, self.token_url, self.certificate_url)
        self.assertEqual(cert, auth.cert)

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    @mock.patch('compute_wps.auth.oauth2.check_certificate')
    def test_get_certificate_status_code_not_200(self, mock_check, mock_oauth2):
        status_code = 999
        auth, test_user = self._create_user_with_auth(self.type)
        self._setup_mock_status_code(mock_check, mock_oauth2, status_code)

        with self.assertRaises(oauth2.OAuth2Error) as e:
            cert = oauth2.get_certificate(test_user, 'http://test.com/refresh',
                                          'http://test.com/certificate')
        self.assertTrue('Error retrieving certificate' in str(e.exception))

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    @mock.patch('compute_wps.auth.oauth2.check_certificate')
    def test_get_certificate_token_updated(self, mock_check, mock_oauth2):
        auth, test_user = self._create_user_with_auth(self.type)
        mock_check.return_value = False
        mock_oauth2.side_effect = TokenUpdated(self.test_token)
        with self.assertRaises(oauth2.TokenUpdated):
            cert = oauth2.get_certificate(test_user, self.token_url, self.certificate_url)
        self.assertEqual(mock_oauth2.call_count, 2)

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    @mock.patch('compute_wps.auth.oauth2.check_certificate')
    def test_get_certificate_exception(self, mock_check, mock_oauth2):
        auth, test_user = self._create_user_with_auth(self.type)
        mock_check.return_value = False
        mock_oauth2.side_effect = Exception('some exception')
        with self.assertRaises((oauth2.OAuth2Error, Exception)) as e:
            cert = oauth2.get_certificate(test_user, self.token_url, self.certificate_url)
        self.assertEqual(str(e.exception), 'OAuth2 authorization required')

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    @mock.patch('compute_wps.auth.oauth2.check_certificate')
    def test_get_certificate_invalid_grant(self, mock_check, mock_oauth2):
        auth, test_user = self._create_user_with_auth(self.type)
        mock_check.return_value = False
        mock_oauth2.side_effect = oauth2.InvalidGrantError
        with self.assertRaises((oauth2.OAuth2Error, oauth2.InvalidGrantError)) as e:
            cert = oauth2.get_certificate(test_user, self.token_url, self.certificate_url)
        self.assertEqual(str(e.exception), 'OAuth2 authorization required')

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_token_exception(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.fetch_token = mock.Mock(side_effect=Exception)

        with self.assertRaises(oauth2.OAuth2Error) as e:
            oauth2.get_token('http://test.com/token', 'http://test.com/request', {})
        self.assertEqual(str(e.exception), 'Failed to retrieve token')

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_token(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.fetch_token = mock.Mock(return_value=self.test_token)

        result = oauth2.get_token('http://test.com/token_url', 'http://test.com/request', {})
        for k in self.test_token:
            self.assertEqual(result[k], self.test_token[k])

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_authorization_url_exception(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.authorization_url = mock.Mock(side_effect=Exception)

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_authorization_url(self.authorization_url, 'authorization')

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_authorization_url(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.authorization_url = mock.Mock(return_value='http://test.com/authorization')

        result = oauth2.get_authorization_url(self.authorization_url, 'authorization')

        self.assertEqual(result, 'http://test.com/authorization')

# python /compute/manage.py test compute_wps.tests.test_oauth2.TestOAuth2.test_get_certificate_token_updated
# python /compute/manage.py test compute_wps.tests.test_oauth2
