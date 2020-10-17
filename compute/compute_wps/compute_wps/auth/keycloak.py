import base64
import logging

import requests
from rest_framework import authentication
from rest_framework import exceptions
from django.conf import settings

from compute_wps import models

logger = logging.getLogger("compute_wps.auth.keycloak")

def authenticate(meta):
    try:
        header = meta["HTTP_AUTHORIZATION"]
    except KeyError:
        return None

    type, access_token = header.split(" ")

    data = token_introspection(access_token)

    user, created = models.User.objects.get_or_create(username=data["username"])

    if created:
        models.Auth.objects.create(openid_url='', user=user)

    return user

class KeyCloakAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        user = authenticate(request.META)

        if user is None:
            return user

        return (user, None)

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
        raise exceptions.WPSError("Could not verify access token")

    data = response.json()

    if "active" in data and not data["active"]:
        raise exceptions.AuthenticationFailed("Access token is no longer valid")

    logger.info("Successfully introspected token")

    return data

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
