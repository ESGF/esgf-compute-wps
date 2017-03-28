from django.test import TestCase

from wps.auth import openid

class OpenIDTest(TestCase):

    def test_find_non_exist_service(self):
        oid = openid.OpenID.parse('https://ceda.ac.uk/openid/jboutte')

        with self.assertRaises(openid.OpenIDError):
            oid.find('hello:world')

    def test_find_service(self):
        oid = openid.OpenID.parse('https://ceda.ac.uk/openid/jboutte')

        service = oid.find('urn:esg:security:oauth:endpoint:authorize')

        self.assertIsNotNone(service)
        self.assertEqual(service.uri, 'https://slcs.ceda.ac.uk/oauth/authorize')
        self.assertEqual(service.local_id, 'https://ceda.ac.uk/openid/jboutte')

    def test_services(self):
        oid = openid.OpenID.parse('https://ceda.ac.uk/openid/jboutte')

        self.assertEqual(len(oid.services), 7)

    def test_bad_url(self):
        with self.assertRaises(openid.OpenIDError):
            openid.OpenID.parse('https://idontexist')

    def test_xrds(self):
        oid = openid.OpenID.parse('https://ceda.ac.uk/openid/jboutte')

        self.assertIsNotNone(oid)
