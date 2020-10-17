import pytest

from rest_framework import exceptions

from compute_wps import models
from compute_wps.auth import keycloak

@pytest.mark.django_db
def test_authenticate(mocker):
    token_intro = mocker.patch.object(keycloak, "token_introspection")
    token_intro.return_value = {
        "username": "user1",
    }

    meta = {}

    user = keycloak.authenticate(meta)

    assert user is None

    meta = {"HTTP_AUTHORIZATION": "Bearer abcd1234"}

    user_get_or_create = mocker.spy(models.User.objects, "get_or_create")
    auth_create = mocker.spy(models.Auth.objects, "create")

    user = keycloak.authenticate(meta)

    assert user.username == "user1"

    user = keycloak.authenticate(meta)

    assert user_get_or_create.call_count == 2
    assert auth_create.call_count == 1

def test_keycloakauthentication(mocker):
    auth = keycloak.KeyCloakAuthentication()

    class Request:
        def META(self):
            return "user1"

    authenticate = mocker.patch.object(keycloak, "authenticate")

    authenticate.return_value = None

    user = auth.authenticate(Request())

    assert user is None

    authenticate.return_value = "user1"

    user = auth.authenticate(Request())

    assert user == ("user1", None)

def test_token_introspection(mocker, settings):
    settings.AUTH_KEYCLOAK_CLIENT_ID = "client_id"
    settings.AUTH_KEYCLOAK_CLIENT_SECRET = "client_secret"
    settings.AUTH_KEYCLOAK_KNOWN = {
        "introspection_endpoint": "https://test.test/introspect"
    }

    post = mocker.patch("requests.post")

    type(post.return_value).ok = mocker.PropertyMock(return_value=True)
    post.return_value.json.return_value = {"active": True}

    data = keycloak.token_introspection("abcd1234")

    assert data == {"active": True}

    post.return_value.json.return_value = {}

    with pytest.raises(exceptions.AuthenticationFailed):
        keycloak.token_introspection("abcd1234")

    post.return_value.json.return_value = {"active": False}

    with pytest.raises(exceptions.AuthenticationFailed):
        keycloak.token_introspection("abcd1234")

    type(post.return_value).ok = mocker.PropertyMock(return_value=False)

    with pytest.raises(exceptions.AuthenticationFailed):
        keycloak.token_introspection("abcd1234")

def test_init(mocker, settings):
    settings.AUTH_KEYCLOAK_URL = "http://test.test/keycloak"
    settings.AUTH_KEYCLOAK_REALM = "test"

    get = mocker.patch("requests.get")

    get.return_value.json.return_value = {"test": "data1"}

    keycloak.init(settings)

    assert settings.AUTH_KEYCLOAK_KNOWN == {"test": "data1"}
