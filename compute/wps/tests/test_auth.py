#! /usr/bin/env python

import os

from django import test

from wps.auth import oauth2
from wps.auth import openid

class TestOpenID(test.TestCase):

    def setUp(self):
        sample_path = os.path.join(os.path.dirname(__file__), 'data', 'openid_sample.txt')

        with open(sample_path, 'r') as f:
            self.sample = f.read()

    def test_find_does_not_exist(self):
        oid = openid.OpenID.parse(self.sample)

        with self.assertRaises(openid.OpenIDError):
            oid.find('doesnotexist')

    def test_find(self):
        oid = openid.OpenID.parse(self.sample)

        mpc = oid.find('urn:esg:security:myproxy-service')

        self.assertIsInstance(mpc, openid.Service)
        self.assertItemsEqual(mpc.types, ['urn:esg:security:myproxy-service'])
        self.assertEqual(mpc.uri, 'socket://slcs1.ceda.ac.uk:7512')
        self.assertEqual(mpc.local_id, 'https://ceda.ac.uk/openid/jboutte')
        self.assertEqual(mpc.priority, '10')

    def test_retrieve_and_parse_bad_url(self):
        with self.assertRaises(openid.OpenIDError):
            openid.OpenID.retrieve_and_parse('testurl')

    def test_parse_bad_string(self):
        with self.assertRaises(openid.OpenIDError):
            openid.OpenID.parse('')

    def test_parse(self):
        oid = openid.OpenID.parse(self.sample)

        self.assertIsNotNone(oid)

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
