#! /usr/bin/env python

import os

from django import test

from wps.auth import oauth2

class TestOAuth2(test.TestCase):

    def test_get_authorization_url(self):
        url = oauth2.get_authorization_url('https://end/auth', 'https://end/cert')

        self.assertIsInstance(url, tuple)
        self.assertEqual(len(url), 2)

    def test_get_token(self):
        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_token('https://end/access', 'https://end/request', {})

    def test_get_certificate(self):
        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_certificate({}, 'https://end/access', 'https://end/cert')
