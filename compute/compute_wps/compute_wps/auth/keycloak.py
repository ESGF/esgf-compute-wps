import json
import base64
import logging

import requests
from rest_framework import authentication
from rest_framework import exceptions as rest_exceptions
from django.conf import settings

import compute_wps
from compute_wps import models
from compute_wps import exceptions

logger = logging.getLogger("compute_wps.auth.keycloak")

class AuthServerResponseError(exceptions.AuthError):
    pass

class AuthNoUserClientError(exceptions.AuthError):
    pass

def client_registration_uri():
    uri = "{}/realms/{}/clients-registrations/default".format(
        settings.AUTH_KEYCLOAK_URL,
        settings.AUTH_KEYCLOAK_REALM)

    return uri

def get_user_client(uri, user):
    uri = "{}/{}".format(uri, user.username)

    try:
        headers = {
            "Authorization": "bearer {}".format(user.keycloakuserclient.access_token),
        }
    except models.KeyCloakUserClient.DoesNotExist:
        logger.info(f"User has no existing client")

        raise AuthNoUserClientError()

    response = requests.get(
        uri,
        headers = headers)

    data = response.json()

    if "error" in data:
        logger.error(f"User client query failed: {data['error_description']} {response.status_code}")

        raise AuthServerResponseError(data["error_description"])

    user.keycloakuserclient.access_token = data["registrationAccessToken"]
    user.keycloakuserclient.save()

    return data

def create_user_client(uri, user):
    access_token = settings.AUTH_KEYCLOAK_REG_ACCESS_TOKEN

    headers = {
        "Content-Type": "application/json",
        "Authorization": "bearer {}".format(access_token),
    }

    request_body = {
        "authorizationServicesEnabled": False,
        "clientId": user.username,
        "consentRequired": False,
        "serviceAccountsEnabled": True,
        "standardFlowEnabled": False,
    }

    response = requests.post(
        uri,
        headers = headers,
        data = json.dumps(request_body))

    data = response.json()

    if "error" in data:
        logger.error(f"User client creation failed: {data['error_description']} {response.status_code}")

        raise AuthServerResponseError(data["error_description"])

    logger.info("User client created")

    models.KeyCloakUserClient.objects.create(user=user, access_token=data["registrationAccessToken"])

    return data

def client_registration(user):
    uri = client_registration_uri()

    try:
        client = get_user_client(uri, user)
    except AuthNoUserClientError:
        client = create_user_client(uri, user)

    return client["clientId"], client["secret"]

def token_introspection(access_token):
    client_id = settings.AUTH_KEYCLOAK_CLIENT_ID
    client_secret = settings.AUTH_KEYCLOAK_CLIENT_SECRET

    url = settings.AUTH_KEYCLOAK_KNOWN['introspection_endpoint']

    logger.info(f"Inspecting token with {url!r}")

    auth = base64.urlsafe_b64encode("{}:{}".format(client_id, client_secret).encode()).decode("ascii")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic {}".format(auth),
    }

    response = requests.post(
        url,
        data={"token": str(access_token)},
        headers=headers)

    logger.debug(f"Introspection status {response.status_code}")

    if not response.ok:
        raise exceptions.AuthError("Could not verify access token")

    data = response.json()

    logger.info(data)

    if "active" not in data or not data["active"]:
        raise exceptions.AuthError("Access token is no longer valid")

    logger.info("Successfully introspected token")

    return data

def authenticate_request(meta):
    try:
        header = meta["HTTP_AUTHORIZATION"]
    except KeyError:
        raise exceptions.AuthError()

    _, token = header.split(" ")

    return authenticate(token)

def authenticate(access_token):
    data = token_introspection(access_token)

    user, _ = models.User.objects.get_or_create(username=data["username"])

    return user

class KeyCloakAuthorizationCode(object):
    def get_user(self, user_id):
        try:
            user = models.User.objects.get(pk=user_id)
        except models.User.DoesNotExist:
            return None

        return user

    def authenticate(self, request, access_token):
        logger.info("Authentication with authorization code")

        return authenticate(access_token)

class KeyCloakAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        try:
            header = request.META["HTTP_AUTHORIZATION"]
        except KeyError:
            return None

        _, access_token = header.split(" ")

        try:
            user = authenticate(access_token)
        except exceptions.AuthError as e:
            raise rest_exceptions.AuthenticationFailed(str(e))

        if user is None:
            raise rest_exceptions.AuthenticationFailed()

        return (user, None)

def init(global_settings):
    url = "{}/realms/{}/.well-known/openid-configuration".format(
        global_settings.AUTH_KEYCLOAK_URL,
        global_settings.AUTH_KEYCLOAK_REALM)

    logger.info(f"Using KeyCloak well known {url!r}")

    response = requests.get(url)

    response.raise_for_status()

    known = response.json()

    setattr(global_settings, "AUTH_KEYCLOAK_KNOWN", known)

    logger.info("Loaded well known document")
