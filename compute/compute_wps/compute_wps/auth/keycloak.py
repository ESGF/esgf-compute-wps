import json
import base64
import logging

import requests
from rest_framework import authentication
from rest_framework import exceptions
from django.conf import settings

import compute_wps
from compute_wps import models

logger = logging.getLogger("compute_wps.auth.keycloak")

class AuthError(Exception):
    pass

class AuthServerResponseError(AuthError):
    pass

class AuthNoUserClientError(AuthError):
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
    # access_token = "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI1NWFjZTdjOC04N2MwLTQ3ZjAtOGQ4My0zNTM3NDU5NTE4YTUifQ.eyJleHAiOjAsImlhdCI6MTYwMzI0MTg0MiwianRpIjoiNjUwMzcwNTMtNGU4OC00ZDdhLWE3ODUtMjgwMWVmMjk2YjEyIiwiaXNzIjoiaHR0cDovLzE5Mi4xNjguODYuMjcva2V5Y2xvYWsvcmVhbG1zL21hc3RlciIsImF1ZCI6Imh0dHA6Ly8xOTIuMTY4Ljg2LjI3L2tleWNsb2FrL3JlYWxtcy9tYXN0ZXIiLCJ0eXAiOiJSZWdpc3RyYXRpb25BY2Nlc3NUb2tlbiIsInJlZ2lzdHJhdGlvbl9hdXRoIjoiYXV0aGVudGljYXRlZCJ9.dMAjL3o4KNHHpEstluR4GpdeV7RLJvZ57rDc7mvyfPg"

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
        "Host": "192.168.86.27",
    }

    print("TOKEN", access_token)

    response = requests.post(
        url,
        data={"token": str(access_token)},
        headers=headers)

    logger.debug(f"Introspection status {response.status_code}")

    if not response.ok:
        raise exceptions.AuthenticationFailed("Could not verify access token")

    data = response.json()

    logger.info(data)

    if "active" not in data or not data["active"]:
        raise exceptions.AuthenticationFailed("Access token is no longer valid")

    logger.info("Successfully introspected token")

    return data

def authenticate_request(meta):
    try:
        header = meta["HTTP_AUTHORIZATION"]
    except KeyError:
        raise compute_wps.exceptions.WPSError("Missing authorization header")

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

        user = authenticate(access_token)

        if user is None:
            raise exceptions.AuthenticationFailed("Unable to authenticate user")

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
