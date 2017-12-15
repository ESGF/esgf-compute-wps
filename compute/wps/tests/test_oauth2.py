#! /usr/bin/env python

import os

import mock
from django import test

from wps.auth import oauth2

class TestOAuth2(test.TestCase):

    @mock.patch('wps.auth.oauth2.OAuth2Session')
    def test_get_authorization_url_exception(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.authorization_url = mock.Mock(side_effect=Exception)

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_authorization_url('http://test.com/authorization', 'http://test.com/certification')

    @mock.patch('wps.auth.oauth2.OAuth2Session')
    def test_get_authorization_url(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.authorization_url = mock.Mock(return_value='http://test.com/authorization')

        result = oauth2.get_authorization_url('http://test.com/authorization', 'http://test.com/certification')

        self.assertEqual(result, 'http://test.com/authorization')

    @mock.patch('wps.auth.oauth2.OAuth2Session')
    def test_get_token_exception(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.fetch_token = mock.Mock(side_effect=Exception)

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_token('http://test.com/token', 'http://test.com/request', {})

    @mock.patch('wps.auth.oauth2.OAuth2Session')
    def test_get_token(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.fetch_token = mock.Mock(return_value='NewToken')

        result = oauth2.get_token('http://test.com/token', 'http://test.com/request', {})

        self.assertEqual(result, 'NewToken')

    @mock.patch('wps.auth.oauth2.OAuth2Session')
    def test_get_certificate_status_code(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.post = mock.Mock(return_value=mock.Mock(status_code=300))

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_certificate('token', 'http://test.com/refresh', 'http://test.com/certificate')

    @mock.patch('wps.auth.oauth2.OAuth2Session')
    def test_get_certificate_exception(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.post = mock.Mock(side_effect=Exception)

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_certificate('token', 'http://test.com/refresh', 'http://test.com/certificate')

    @mock.patch('wps.auth.oauth2.OAuth2Session')
    def test_get_certificate(self, oauth2_mock):
        session_mock = oauth2_mock.return_value

        session_mock.post = mock.Mock(return_value=mock.Mock(status_code=200, text='certificate'))

        result = oauth2.get_certificate('token', 'http://test.com/refresh', 'http://test.com/certificate')

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], 'certificate')
        self.assertEqual(result[2], 'token')

    def test_get_env_doesnt_exist(self):
        with self.assertRaises(Exception):
            oauth2.get_env('DONT')

    def test_get_env(self):
        path = oauth2.get_env('PATH') 
