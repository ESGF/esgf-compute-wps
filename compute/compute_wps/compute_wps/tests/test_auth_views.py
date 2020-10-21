import pytest
from oauthlib import oauth2
from django.contrib.sessions import middleware

from compute_wps import models
from compute_wps.views import auth
from compute_wps.auth import keycloak


def test_client_registration(rf, mocker):
    request = rf.get("/auth/client_registration/")
    request.user = mocker.MagicMock()

    type(request.user).is_authenticate = mocker.PropertyMock(return_value=False)

    client_registration = mocker.patch(
        "compute_wps.auth.keycloak.client_registration")
    client_registration.side_effect = keycloak.AuthError()

    response = auth.client_registration(request)

    assert response.status_code == 500

    client_registration = mocker.patch(
        "compute_wps.auth.keycloak.client_registration")
    client_registration.return_value = ("client_id", "client_secret")

    response = auth.client_registration(request)

    assert response.status_code == 200

def test_generate_nonce(mocker):
    random = mocker.patch("random.randint")
    random.side_effect = [x for x in range(64)]

    nonce = auth.generate_nonce()

    assert nonce == "0123456789101112131415161718192021222324252627282930313233343536373839404142434445464748495051525354555657585960616263"

@pytest.mark.django_db
def test_login(rf, mocker, settings):
    settings.AUTH_KEYCLOAK_URL = "http://keycloak.local/keycloak"
    settings.AUTH_KEYCLOAK_KNOWN = {
        "authorization_endpoint": "http://keycloak.local/keycloak/auth",
    }

    request = rf.get("/auth/login/", {"next": "http://dev.local/next"})

    session_middleware = middleware.SessionMiddleware()
    session_middleware.process_request(request)
    request.session.save()

    response = auth.login(request)

    assert response.status_code == 302
    assert "http://keycloak.local/keycloak/auth" in response.url

@pytest.mark.django_db
def test_get_access_token(rf, mocker):
    request = rf.get("/auth/oauth_callback/", {"code": "abcd1234", "state": "abcd"})

    post = mocker.patch("requests.post")

    client = oauth2.WebApplicationClient("client_id_1234")

    nonce = models.Nonce.objects.create(
        state="abcd",
        redirect_uri="http://dev.local/redirect",
        next="http://dev.local/next")

    token = auth.get_access_token(client, request, nonce)

    assert token

@pytest.mark.django_db
def test_login_complete(rf, mocker):
    request = rf.get("/auth/oauth_callback/")

    session_middleware = middleware.SessionMiddleware()
    session_middleware.process_request(request)
    request.session.save()

    response = auth.login_complete(request)

    assert response.status_code == 500
    assert response.content == b""

    request.session["oidc_state"] = "abcd"
    request.session.save()

    response = auth.login_complete(request)

    assert response.status_code == 500
    assert response.content == b""

    request.session["oidc_state"] = "abcd"
    request.session.save()
    nonce = models.Nonce.objects.create(
        state="abcd",
        redirect_uri="http://dev.local/redirect",
        next="http://dev.local/next")

    get_access_token = mocker.patch("compute_wps.views.auth.get_access_token")
    get_access_token.side_effect = Exception

    response = auth.login_complete(request)

    assert response.status_code == 500
    assert response.content == b""

    request.session["oidc_state"] = "abcd"
    request.session.save()
    nonce = models.Nonce.objects.create(
        state="abcd",
        redirect_uri="http://dev.local/redirect",
        next="http://dev.local/next")

    get_access_token = mocker.patch("compute_wps.views.auth.get_access_token")
    get_access_token.return_value = {
        "error": "something went down",
    }

    response = auth.login_complete(request)

    assert response.status_code == 500
    assert response.content == b"something went down"

    django_auth = mocker.patch("django.contrib.auth.authenticate")
    django_auth.return_value = None

    request.session["oidc_state"] = "abcd"
    request.session.save()
    nonce = models.Nonce.objects.create(
        state="abcd",
        redirect_uri="http://dev.local/redirect",
        next="http://dev.local/next")

    get_access_token = mocker.patch("compute_wps.views.auth.get_access_token")
    get_access_token.return_value = {
        "access_token": "abcd1234",
    }

    response = auth.login_complete(request)

    assert response.status_code == 302
    assert response.url == "/auth/login/"

    user = models.User.objects.create(username="user1")

    django_auth = mocker.patch("django.contrib.auth.authenticate")
    django_auth.return_value = user

    request.session["oidc_state"] = "abcd"
    request.session.save()
    nonce = models.Nonce.objects.create(
        state="abcd",
        redirect_uri="http://dev.local/redirect",
        next="http://dev.local/next")

    get_access_token = mocker.patch("compute_wps.views.auth.get_access_token")
    get_access_token.return_value = {
        "access_token": "abcd1234",
    }

    login = mocker.patch("django.contrib.auth.login")

    response = auth.login_complete(request)

    assert response.status_code == 302
    assert response.url == "http://dev.local/next"

    login.assert_called_with(request, user)
