import os
import json
import random
import logging

import requests
from oauthlib import oauth2
from django import http
from django import shortcuts
from django.conf import settings
from django.contrib import auth as django_auth
from django.contrib.auth.decorators import login_required

from compute_wps import models
from compute_wps.auth import keycloak

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "false"

logger = logging.getLogger("compute_wps.views.auth")

@login_required(login_url="/auth/login/")
def client_registration(request):
    try:
        client_id, client_secret = keycloak.client_registration(request.user)
    except keycloak.AuthError as e:
        return http.HttpResponseServerError(str(e))

    template = f"""
<html>
    <body>
        <b><h2>Keep your "Client Secret" secure.</h2></b>
        <pre>
Client ID: {client_id}
Client Secret: {client_secret}
        </pre>
    </body>
</html>
    """

    return http.HttpResponse(template)

def generate_nonce():
    return "".join([str(random.randint(0, 27)) for i in range(64)])

def login(request):
    uri = settings.AUTH_KEYCLOAK_KNOWN["authorization_endpoint"]

    client = oauth2.WebApplicationClient(settings.AUTH_KEYCLOAK_CLIENT_ID)

    completed_redirect = request.build_absolute_uri("/auth/oauth_callback/")

    nonce = models.Nonce.objects.create(
        state=generate_nonce(),
        redirect_uri=completed_redirect,
        next=request.GET["next"])

    request.session["oidc_state"] = nonce.state

    redirect_url = client.prepare_request_uri(
        uri,
        redirect_uri=nonce.redirect_uri,
        state=nonce.state)

    return shortcuts.redirect(redirect_url)

def get_access_token(client, request, nonce):
    response = client.parse_request_uri_response(request.get_full_path(), state=nonce.state)

    request_body = client.prepare_request_body(
        response["code"],
        redirect_uri=nonce.redirect_uri,
        client_secret=settings.AUTH_KEYCLOAK_CLIENT_SECRET)

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(
        settings.AUTH_KEYCLOAK_KNOWN["token_endpoint"],
        data=request_body,
        headers=headers)

    return response.json()

def login_complete(request):
    client = oauth2.WebApplicationClient(settings.AUTH_KEYCLOAK_CLIENT_ID)

    try:
        nonce = models.Nonce.objects.get(state=request.session["oidc_state"])
    except (models.Nonce.DoesNotExist, KeyError):
        logger.error("Could not retrieve nonce")

        return http.HttpResponseServerError()

    try:
        data = get_access_token(client, request, nonce)
    except Exception as e:
        logger.error("Could not get access token")

        return http.HttpResponseServerError(str(e))
    finally:
        nonce.delete()

        del request.session["oidc_state"]

    if "error" in data:
        logger.error("Error from keycloak server")

        return http.HttpResponseServerError(data["error"])

    user = django_auth.authenticate(request, access_token=data["access_token"])

    if user is not None:
        django_auth.login(request, user)

        return shortcuts.redirect(nonce.next)

    return shortcuts.redirect("/auth/login/")
