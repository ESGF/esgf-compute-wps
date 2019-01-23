#! /usr/bin/env python

import logging
import os
from base64 import b64encode

from django.conf import settings
from OpenSSL import crypto
from requests_oauthlib import OAuth2Session

logger = logging.getLogger('wps.auth.oauth2')

class OAuth2Error(Exception):
    pass

def get_env(key):
    try:
        return os.environ[key]
    except KeyError:
        raise OAuth2Error('Environment variable "{}" has not been set'.format(key))

def get_certificate(token, state, refresh_url, cert_url, refresh=False):
    client_id = get_env('OAUTH_CLIENT')

    secret = get_env('OAUTH_SECRET')

    slcs = OAuth2Session(client_id,
                         token=token,
                         state=state)

    #slcs = OAuth2Session(client_id,
    #                     token=token,
    #                     state=state,
    #                     auto_refresh_url=refresh_url,
    #                     auto_refresh_kwargs = {
    #                                            'client_id': client_id,
    #                                            'client_secret': secret,
    #                                           },
    #                     token_updater = lambda t: token.update(t))

    key_pair = crypto.PKey()
    key_pair.generate_key(crypto.TYPE_RSA, 2048)

    private_key = crypto.dump_privatekey(crypto.FILETYPE_PEM, key_pair).decode('utf-8')

    cert_request = crypto.X509Req()
    cert_request.set_pubkey(key_pair)
    cert_request.sign(key_pair, 'md5')
    cert_request = crypto.dump_certificate_request(crypto.FILETYPE_ASN1, cert_request)

    if cert_url[-1] != '/':
        cert_url = '{}/'.format(cert_url)

    headers = {
        'Referer': 'same-origin',
    }

    if refresh:
        new_token = slcs.refresh_token(refresh_url, headers=headers, verify=False)

        token.update(new_token)

    logger.info('Attempting to get CSRF token from server')

    # Grab a CSRF token
    try:
        response = slcs.get(cert_url, verify=False, headers=headers)
    except Exception:
        logger.exception('CSRF Token grab')

        csrftoken = None
    else:
        csrftoken = slcs.cookies['csrftoken']

    headers['X-CSRFToken'] = csrftoken

    try:
        response = slcs.post(cert_url,
                             data={ 'certificate_request': b64encode(cert_request) },
                             verify=False,
                             headers=headers)
    except Exception:
        raise OAuth2Error('Failed to contact authentication server.')

    if response.status_code != 200:
        raise OAuth2Error('Failed to retrieve certificate {}'.format(response.reason))

    return response.text, private_key, token

def get_token(token_uri, request_url, oauth_state):
    client_id = get_env('OAUTH_CLIENT')

    secret = get_env('OAUTH_SECRET')

    slcs = OAuth2Session(client_id,
            redirect_uri=settings.WPS_OAUTH2_CALLBACK,
            state=oauth_state)

    try:
        token = slcs.fetch_token(token_uri,
                                 client_secret=secret,
                                 authorization_response=request_url,
                                 verify=False)
    except Exception:
        raise OAuth2Error('Failed to retrieve token')

    return token

def get_authorization_url(auth_uri, cert_uri):
    client_id = get_env('OAUTH_CLIENT')

    if cert_uri[-1] != '/':
        cert_uri = '{}/'.format(cert_uri)

    slcs = OAuth2Session(client_id,
            redirect_uri=settings.WPS_OAUTH2_CALLBACK,
            scope=[cert_uri])

    try:
        return slcs.authorization_url(auth_uri)
    except Exception:
        raise OAuth2Error('Failed to retrieve authorization url')
