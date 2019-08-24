#! /usr/bin/env python

import os

import mock
from django import test

from compute_wps import models
from compute_wps.auth import oauth2
from . import helpers

class TestOAuth2(test.TestCase):
    fixtures = ['users.json']

    def setUp(self):
        os.environ['OAUTH_CLIENT'] = ''
        os.environ['OAUTH_SECRET'] = ''
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

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_authorization_url_exception(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.authorization_url = mock.Mock(side_effect=Exception)

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_authorization_url('http://test.com/authorization', 'http://test.com/certification')

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_authorization_url(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.authorization_url = mock.Mock(return_value='http://test.com/authorization')

        result = oauth2.get_authorization_url('http://test.com/authorization', 'http://test.com/certification')

        self.assertEqual(result, 'http://test.com/authorization')

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_token_exception(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.fetch_token = mock.Mock(side_effect=Exception)

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_token('http://test.com/token', 'http://test.com/request', {})

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_token(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.fetch_token = mock.Mock(return_value='NewToken')

        result = oauth2.get_token('http://test.com/token', 'http://test.com/request', {})

        self.assertEqual(result, 'NewToken')

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_certificate_status_code(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.post = mock.Mock(return_value=mock.Mock(status_code=300))

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_certificate(self.user, 'http://test.com/refresh', 'http://test.com/certificate')

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_certificate_exception(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.post = mock.Mock(side_effect=Exception)

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_certificate('token', 'state', 'http://test.com/refresh', 'http://test.com/certificate')

    @mock.patch('compute_wps.auth.oauth2.OAuth2Session')
    def test_get_certificate(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.post = mock.Mock(return_value=mock.Mock(status_code=200, text='certificate'))

        result = oauth2.get_certificate(self.user, 'http://test.com/refresh', 'http://test.com/certificate')

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], 'certificate')
        self.assertEqual(result[2], 'token')

    def test_get_env_doesnt_exist(self):
        with self.assertRaises(Exception):
            oauth2.get_env('DONT')

    def test_get_env(self):
        path = oauth2.get_env('PATH') 
