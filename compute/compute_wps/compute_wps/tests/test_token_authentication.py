
import time
import mock
from django import test

from compute_wps import models
from compute_wps.auth import token_authentication
from compute_wps.auth.token_authentication import exceptions

class TokenAuthenticationTestCase(test.TestCase):
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

    user_name = "test_user1"

    def setUp(self):
        self.test_user = self._create_user_with_auth(type)

    def _create_user_with_auth(self, auth_type):
        test_user = models.User.objects.create_user(self.user_name,
                                                         'test_email@test.com', 'test_password1')        
        auth = models.Auth.objects.create(openid_url=self.openid_url, user=test_user)
        auth.update(auth_type, self.certs, self.api_key, token=self.test_token, state='good_state')
        return test_user

    def test_authenticate(self):
        ta = token_authentication.TokenAuthentication()
        request = mock.MagicMock()
        type(request).META = mock.MagicMock()
        type(request).META.get.return_value = self.api_key
        user, dummy = ta.authenticate(request)
        self.assertEqual(user, self.test_user)

    def test_authenticate_user_not_exist(self):
        ta = token_authentication.TokenAuthentication()
        request = mock.MagicMock()
        type(request).META = mock.MagicMock()
        type(request).META.get.return_value = "dummy"
        with self.assertRaises(exceptions.AuthenticationFailed):
            user, dummy = ta.authenticate(request)

    def test_authenticate_no_token(self):
        ta = token_authentication.TokenAuthentication()
        request = mock.MagicMock()
        type(request).META = mock.MagicMock()
        type(request).META.get.return_value = None
        ret_val = ta.authenticate(request)
        self.assertEqual(ret_val, None)

