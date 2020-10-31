import json
import pytest

from rest_framework import exceptions as rest_exceptions

import compute_wps
from compute_wps import models
from compute_wps.auth import keycloak
from compute_wps import exceptions

@pytest.mark.django_db
def test_client_registration(mocker):
    user = models.User.objects.create(username="user1")

    get = mocker.patch.object(keycloak, "get_user_client")
    create = mocker.patch.object(keycloak, "create_user_client")

    get.side_effect = keycloak.AuthNoUserClientError

    create.return_value = {
        "clientId": "user1",
        "secret": "token",
    }

    client_id, client_secret = keycloak.client_registration(user)

    assert client_id == "user1"
    assert client_secret == "token"

@pytest.mark.django_db
def test_create_user_client(mocker, settings):
    settings.AUTH_KEYCLOAK_REG_ACCESS_TOKEN = "reg_abcd1234"

    uri = "http://auth.local/keycloak/realms/master/clients-registrations/default"
    user = models.User.objects.create(username="user1")

    post = mocker.patch("requests.post")
    post.return_value.json.return_value = {
        "error": "error code",
        "error_description": "error text",
    }

    with pytest.raises(keycloak.AuthServerResponseError):
        keycloak.create_user_client(uri, user)

    post.return_value.json.return_value = {
        "registrationAccessToken": "reg_1234abcd",
    }

    data = keycloak.create_user_client(uri, user)

    post.assert_called_with(
        uri,
        headers={
            "Content-Type": "application/json",
            "Authorization": "bearer reg_abcd1234",
        },
        data=json.dumps({
            "authorizationServicesEnabled": False,
            "clientId": "user1",
            "consentRequired": False,
            "serviceAccountsEnabled": True,
            "standardFlowEnabled": False,
        }))

    assert data == {
        "registrationAccessToken": "reg_1234abcd",
    }

@pytest.mark.django_db
def test_get_user_client(mocker):
    uri = "http://auth.local/keycloak/realms/master/clients-registrations/default"
    user = models.User.objects.create(username="user1")

    with pytest.raises(keycloak.AuthNoUserClientError):
        keycloak.get_user_client(uri, user)

    models.KeyCloakUserClient.objects.create(user=user, access_token="abcd1234")

    get = mocker.patch("requests.get")
    get.return_value.json.return_value = {
        "error": "error code",
        "error_description": "error text",
    }

    with pytest.raises(keycloak.AuthServerResponseError):
        keycloak.get_user_client(uri, user)

    get.return_value.json.return_value = {
        "registrationAccessToken": "1234abcd",
        "clientId": "user1",
        "secret": "new",
    }

    client = keycloak.get_user_client(uri, user)

    get.assert_called_with(f"{uri}/user1", headers={"Authorization": "bearer abcd1234"})

    assert client == {
        "registrationAccessToken": "1234abcd",
        "clientId": "user1",
        "secret": "new",
    }

    assert user.keycloakuserclient.access_token == "1234abcd"

def test_client_registration_uri(settings):
    settings.AUTH_KEYCLOAK_URL = "http://auth.local/keycloak"
    settings.AUTH_KEYCLOAK_REALM = "master"

    uri = keycloak.client_registration_uri()

    assert uri == "http://auth.local/keycloak/realms/master/clients-registrations/default"

@pytest.mark.django_db
def test_authenticate(mocker):
    token_intro = mocker.patch.object(keycloak, "token_introspection")
    token_intro.return_value = {
        "username": "user1",
        "client_id": "user1-service",
    }

    user_get_or_create = mocker.spy(models.User.objects, "get_or_create")

    user = keycloak.authenticate("abcd1234")

    assert user is not None
    assert user.username == "user1"
    assert user_get_or_create.call_count == 1

@pytest.mark.django_db
def test_authenticate_request(mocker):
    token_intro = mocker.patch.object(keycloak, "token_introspection")
    token_intro.return_value = {
        "username": "user1",
        "client_id": "user1-service",
    }

    user_get_or_create = mocker.spy(models.User.objects, "get_or_create")

    user = keycloak.authenticate_request({"HTTP_AUTHORIZATION": "bearer abcd1234"})

    assert user is not None
    assert user.username == "user1"
    assert user_get_or_create.call_count == 1

    with pytest.raises(exceptions.AuthError):
        keycloak.authenticate_request({})

@pytest.mark.django_db
def test_keycloakauthorizationcode(mocker):
    auth = keycloak.KeyCloakAuthorizationCode()

    user = auth.get_user(1)

    assert user is None

    u = models.User.objects.create(username="user1")

    user = auth.get_user(u.pk)

    assert user == u

    authenticate = mocker.patch.object(keycloak, "authenticate")

    user = auth.authenticate(None, "tokenabcd1234")

    authenticate.assert_called_with("tokenabcd1234")

def test_keycloakauthentication(mocker, settings):
    settings.AUTH_KEYCLOAK = True

    auth = keycloak.KeyCloakAuthentication()

    class Request:
        META = {}

    user = auth.authenticate(Request())

    assert user is None

    class Request:
        META = {"HTTP_AUTHORIZATION": "bearer abcd1234"}

    authenticate = mocker.patch.object(keycloak, "authenticate")

    authenticate.return_value = None

    with pytest.raises(rest_exceptions.AuthenticationFailed):
        auth.authenticate(Request())

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

    with pytest.raises(exceptions.AuthError):
        keycloak.token_introspection("abcd1234")

    post.return_value.json.return_value = {"active": False}

    with pytest.raises(exceptions.AuthError):
        keycloak.token_introspection("abcd1234")

    type(post.return_value).ok = mocker.PropertyMock(return_value=False)

    with pytest.raises(exceptions.AuthError):
        keycloak.token_introspection("abcd1234")

def test_init(mocker, settings):
    settings.AUTH_KEYCLOAK_URL = "http://test.test/keycloak"
    settings.AUTH_KEYCLOAK_REALM = "test"

    get = mocker.patch("requests.get")

    get.return_value.json.return_value = {"test": "data1"}

    keycloak.init(settings)

    assert settings.AUTH_KEYCLOAK_KNOWN == {"test": "data1"}
