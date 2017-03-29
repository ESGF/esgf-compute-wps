#! /usr/bin/env python

import os

from requests_oauthlib import OAuth2Session

from wps import settings

class OAuth2Error(Exception):
    pass

def get_env(key):
    try:
        return os.getenv(key)
    except KeyError:
        raise OAuth2Error('Environment variable "{}" has not been set'.format(key))

def get_token(token_uri, request_url, oauth_state):
    client_id = get_env('OAUTH_CLIENT')

    secret = get_env('OAUTH_SECRET')

    slcs = OAuth2Session(client_id,
            redirect_uri=settings.OAUTH2_CALLBACK,
            state=oauth_state)

    try:
        token = slcs.fetch_token(token.uri,
                client_secret=secret,
                authorization_response=request_url)
    except Exception as e:
        raise OAuth2Error('Failed to fetch token')

    return token

def get_authorization_url(auth_uri, cert_uri):
    client_id = get_env('OAUTH_CLIENT')

    if cert_uri[-1] != '/':
        cert_uri = '{}/'.format(cert_uri)

    slcs = OAuth2Session(client_id,
            redirect_uri=settings.OAUTH2_CALLBACK,
            scope=[cert_uri])

    try:
        return slcs.authorization_url(auth_uri)
    except Exception:
        raise OAuth2Error('Failed to retrieve authorization url')
