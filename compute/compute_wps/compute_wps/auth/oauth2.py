#! /usr/bin/env python

import logging
import os
from base64 import b64encode
from datetime import datetime

from OpenSSL import crypto
from django.conf import settings
from oauthlib.oauth2.rfc6749.errors import InvalidGrantError
from requests_oauthlib import OAuth2Session
from requests_oauthlib import TokenUpdated

from compute_wps.exceptions import WPSError

logger = logging.getLogger('wps.auth.oauth2')

# Overrides for OAuth2 endpoints
OAUTH2_AUTHORIZE_URL = os.environ.get('OAUTH2_AUTHORIZE_URL', None)
OAUTH2_TOKEN_URL = os.environ.get('OAUTH2_TOKEN_URL', None)
OAUTH2_CERTIFICATE_URL = os.environ.get('OAUTH2_CERTIFICATE_URL', None)

# REQUIRED client id an client secret
OAUTH2_CLIENT_ID = os.environ['OAUTH2_CLIENT_ID']
OAUTH2_CLIENT_SECRET = os.environ['OAUTH2_CLIENT_SECRET']

# Override for OAuth2 callback
OAUTH2_CALLBACK_URL = os.environ.get('OAUTH2_CALLBACK_URL', settings.OAUTH2_CALLBACK_URL)

CERT_DATE_FMT = '%Y%m%d%H%M%SZ'


class OAuth2Error(WPSError):
    pass


class CertificateError(WPSError):
    def __init__(self, user, reason):
        msg = 'Certificate error for user "{username}": {reason}'

        super(CertificateError, self).__init__(msg, username=user.username, reason=reason)


def check_certificate(user):
    """ Check user certificate.

    Loads current certificate and checks if it has valid date range.

    Args:
        user: User object.

    Return:
        returns true if valid otherwise false.
    """
    if user.auth.cert is None or user.auth.cert == '':
        return False

    try:
        cert = crypto.load_certificate(crypto.FILETYPE_PEM, user.auth.cert)
    except Exception:
        raise CertificateError(user, 'Loading certificate')

    before = datetime.strptime(cert.get_notBefore().decode('utf-8'), CERT_DATE_FMT)

    after = datetime.strptime(cert.get_notAfter().decode('utf-8'), CERT_DATE_FMT)

    now = datetime.now()

    if now >= after:
        logger.info('Certificate not valid after %r, now %r', after, now)

        return False

    if now <= before:
        logger.info('Certificate not valid before %r, now %r', before, now)

        return False

    return True


def refresh_certificate(user, token_url, certificate_url):
    """ Refresh user certificate

    Will try to refresh a users certificate if authenticated using OAuth2.

    Args:
        user: User object.

    Return:
        returns new certificate
    """
    logger.info('Refreshing user certificate')

    if user.auth.type == 'myproxyclient':
        raise CertificateError(user, 'MyProxyClient certificate has expired')

    token, oauth2_state = user.auth.get('token', 'state')

    certs = get_certificate(token, oauth2_state, token_url, certificate_url)

    return certs


def generate_certificate_request():
    key_pair = crypto.PKey()
    key_pair.generate_key(crypto.TYPE_RSA, 2048)

    private_key = crypto.dump_privatekey(crypto.FILETYPE_PEM, key_pair).decode('utf-8')

    request = crypto.X509Req()
    request.set_pubkey(key_pair)
    request.sign(key_pair, 'md5')

    certificate_request = crypto.dump_certificate_request(crypto.FILETYPE_ASN1, request)

    return private_key, certificate_request


def get_certificate(user, refresh_url, certificate_url):
    if not check_certificate(user):
        logger.info('Generating a new certificate')

        private_key, certificate_request = generate_certificate_request()

        if certificate_url[-1] != '/':
            certificate_url = '{!s}/'.format(certificate_url)

        headers = {
            'Referer': refresh_url
        }

        data = {
            'certificate_request': b64encode(certificate_request)
        }

        token, state = user.auth.get('token', 'state')

        logger.info('Using token %r and state %r', token, state)

        kwargs = {
            'token': token,
            # 'state': state,
            'auto_refresh_url': refresh_url,
            'auto_refresh_kwargs': {
                'client_id': OAUTH2_CLIENT_ID,
                'client_secret': OAUTH2_CLIENT_SECRET,
            }
        }

        try:
            slcs = OAuth2Session(OAUTH2_CLIENT_ID, **kwargs)

            response = slcs.post(certificate_url, data=data, verify=False, headers=headers)

            # Clear the token so it doesn't get updated
            kwargs['token'] = None
        except TokenUpdated as e:
            # Update the token so it gets stored in the database
            kwargs['token'] = e.token

            logger.info('Refreshed token %r', kwargs['token'])

            slcs = OAuth2Session(OAUTH2_CLIENT_ID, token=kwargs['token'], state=state)

            response = slcs.post(certificate_url, data=data, verify=False, headers=headers)
        except (Exception or InvalidGrantError):
            logger.exception('Error retrieving certificate')

            raise OAuth2Error('OAuth2 authorization required')

        if response.status_code != 200:
            raise OAuth2Error('Error retrieving certificate {!s} {!s}'.format(response.status_code, response.reason))

        user.auth.update(certs=[response.text, private_key], token=kwargs['token'])

    return user.auth.cert


def get_token(token_url, request_url, oauth_state):
    request_url = request_url.replace('http://', 'https://')

    logger.info('Request URL %r', request_url)

    slcs = OAuth2Session(OAUTH2_CLIENT_ID, redirect_uri=OAUTH2_CALLBACK_URL, state=oauth_state)

    kwargs = {
        'client_secret': OAUTH2_CLIENT_SECRET,
        'authorization_response': request_url,
        'verify': False,
    }

    try:
        return slcs.fetch_token(token_url, **kwargs)
    except Exception:
        raise OAuth2Error('Failed to retrieve token')


def get_authorization_url(authorization_url, scope):
    if scope[-1] != '/':
        scope = '{!s}/'.format(scope)

    logger.info('Using scope %r and redirecting to %r', scope, OAUTH2_CALLBACK_URL)

    slcs = OAuth2Session(OAUTH2_CLIENT_ID, redirect_uri=OAUTH2_CALLBACK_URL, scope=[scope])

    logger.info('Generating authorization url from %r', authorization_url)

    try:
        return slcs.authorization_url(authorization_url, verify=False)
    except Exception:
        raise OAuth2Error('Failed to retrieve authorization url')
