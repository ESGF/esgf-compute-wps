#! /usr/bin/env python

import os

from requests_oauthlib import OAuth2Session

from wps import settings
from wps.auth import openid

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_ACCESS = 'urn:esg:security:oauth:endpoint:access'
URN_CERTIFICATE = 'urn:esg:security:oauth:endpoint:resource'

class OAuth2Error(Exception):
    pass

def get_env(key):
    try:
        return os.getenv(key)
    except KeyError:
        raise OAuth2Error('Environment variable "{}" has not been set'.format(key))

def endpoints_from_openid(openid_url):
    try:
        oid = openid.OpenID.parse(openid_url)
    except openid.OpenIDError:
        raise OAuth2Error('Failed to parse OpenID metadata')

    try:
        auth = oid.find(URN_AUTHORIZE)

        access = oid.find(URN_ACCESS)

        cert = oid.find(URN_CERTIFICATE)
    except openid.OpenIDError:
        raise OAuth2Error('Failed to find OAuth2 endpoint')

    return auth, access, cert

def token_from_openid(openid_url, request_url, oauth_state):
    client_id = get_env('OAUTH_CLIENT')

    secret = get_env('OAUTH_SECRET')

    _, token, _ = endpoints_from_openid(openid_url)

    slcs = OAuth2Session(client_id,
            redirect_uri=settings.OAUTH2_CALLBACK,
            state=oauth_state)

    try:
        token = slcs.fetch_token(token.uri,
                client_secret=secret,
                authorization_response=request_url)
    except Exception as e:
        raise OAuth2Error('Failed to fetch token: {}'.format(e.message))

    return token

def auth_url_from_openid(openid_url):
    auth, _, cert = endpoints_from_openid(openid_url)

    client_id = get_env('OAUTH_CLIENT')

    if cert.uri[-1] != '/':
        cert.uri = '{}/'.format(cert.uri)

    slcs = OAuth2Session(client_id,
            redirect_uri=settings.OAUTH2_CALLBACK,
            scope=[cert.uri])

    return slcs.authorization_url(auth.uri)
