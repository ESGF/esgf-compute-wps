from django import test
from django import forms

from wps.auth import openid
from wps.auth import oauth2

URN_AUTH = 'urn:esg:security:oauth:endpoint:authorize'
URN_ACCESS = 'urn:esg:security:oauth:endpoint:access'
URN_CERT = 'urn:esg:security:oauth:endpoint:resource'

class OAuth2Test(test.TestCase):

    def setUp(self):
        self.oid = openid.OpenID.parse('https://ceda.ac.uk/openid/jboutte')

    def test_token_malformed(self):
        access = self.oid.find(URN_ACCESS)

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_token(access.uri, 'http://thisisbad', 'badstate')

    def test_authorization_url_bad_uris(self):
        auth = self.oid.find(URN_AUTH)

        cert = self.oid.find(URN_CERT)

        with self.assertRaises(oauth2.OAuth2Error):
            oauth2.get_authorization_url('http://thisisbad', cert.uri)

        auth_url, state = oauth2.get_authorization_url(auth.uri, 'http://thisisbad')

        self.assertIsNotNone(auth_url)

    def test_authorization_url(self):
        auth = self.oid.find(URN_AUTH)

        cert = self.oid.find(URN_CERT)

        auth_url, state = oauth2.get_authorization_url(auth.uri, cert.uri)

        self.assertIn('https://slcs.ceda.ac.uk/oauth/authorize', auth_url)

class OpenIDTest(test.TestCase):
    
    def test_find_service_does_not_exist(self):
        oid = openid.OpenID.parse('https://ceda.ac.uk/openid/jboutte')

        with self.assertRaises(openid.OpenIDError):
            service = oid.find('hello, world')

    def test_find_service(self):
        oid = openid.OpenID.parse('https://ceda.ac.uk/openid/jboutte')

        service = oid.find(URN_AUTH)

        self.assertIsNotNone(service)
        self.assertEqual(service.uri, 'https://slcs.ceda.ac.uk/oauth/authorize')
        self.assertEqual(service.local_id, 'https://ceda.ac.uk/openid/jboutte')

    def test_openid_parse_bad_url(self):
        with self.assertRaises(openid.OpenIDError):
            oid = openid.OpenID.parse('http://idonotexist.com')

        with self.assertRaises(openid.OpenIDError):
            oid = openid.OpenID.parse('thisisbad')

    def test_openid_parse(self):
        oid = openid.OpenID.parse('https://ceda.ac.uk/openid/jboutte')

        self.assertIsNotNone(oid)
        self.assertEqual(len(oid.services), 7)

class AuthenticationViewsTest(test.TestCase):

    def test_callback(self):
        response = self.client.get('/auth/callback/')

        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, '/auth/login/')

    def test_login_post(self):
        params = {
            'openid': 'https://ceda.ac.uk/openid/jboutte',
        }

        response = self.client.post('/auth/login', params, follow=True)

        self.assertEqual(response.status_code, 200)
        self.assertRedirects(response, '/auth/login/', status_code=301)

    def test_login_get(self):
        response = self.client.get('/auth/login/')

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'wps/login.html')
        self.assertFieldOutput(forms.CharField,
                               { 'https://ceda.ac.uk/openid/jboutte': 'https://ceda.ac.uk/openid/jboutte' },
                               {})
