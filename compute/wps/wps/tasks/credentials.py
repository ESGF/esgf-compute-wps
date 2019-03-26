#! /usr/bin/env python

from builtins import str
import json
import logging
import os
from datetime import datetime

from django.conf import settings
from openid.consumer import discover
from OpenSSL import crypto

from wps import WPSError
from wps.auth import oauth2
from wps.auth import openid

__ALL__ = [
    'check_certificate',
    'refresh_certificate',
    'load_certificate',
]

logger = logging.getLogger('wps.tasks.credentials')

CERT_DATE_FMT = '%Y%m%d%H%M%SZ'

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_RESOURCE = 'urn:esg:security:oauth:endpoint:resource'

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
    if user.auth.cert == '':
        return False

    try:
        cert = crypto.load_certificate(crypto.FILETYPE_PEM, user.auth.cert)
    except Exception as e:
        raise CertificateError(user, 'Loading certificate')

    before = datetime.strptime(cert.get_notBefore(), CERT_DATE_FMT)

    after = datetime.strptime(cert.get_notAfter(), CERT_DATE_FMT)

    now = datetime.now()

    if now >= after:
        logger.info('Certificate not valid after %r, now %r', after, now)

        return False

    if now <= before:
        logger.info('Certificate not valid before %r, now %r', before, now)

        return False

    return True

def refresh_certificate(user):
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

    url, services = discover.discoverYadis(user.auth.openid_url)

    auth_service = openid.find_service_by_type(services, URN_AUTHORIZE)

    cert_service = openid.find_service_by_type(services, URN_RESOURCE)

    try:
        extra = json.loads(user.auth.extra)
    except ValueError as e:
        raise WPSError('Missing OAuth2 state, try authenticating with OAuth2 again')

    if 'token' not in extra:
        raise WPSError('Missing OAuth2 token, try authenticating with OAuth2 again')

    try:
        cert, key, new_token = oauth2.get_certificate(extra['token'],
                                                      extra['state'],
                                                      auth_service.server_url, cert_service.server_url, 
                                                      refresh=True) 
    except KeyError as e:
        raise WPSError('Missing OAuth2 {!r}', e)

    logger.info('Retrieved certificate and new token')

    extra['token'] = new_token

    user.auth.extra = json.dumps(extra)

    user.auth.cert = ''.join([cert, key])

    user.auth.save()

    return user.auth.cert

def load_certificate(user):
    """ Loads a user certificate.

    First the users certificate is checked and refreshed if needed. It's
    then written to disk and the processes current working directory is 
    set, allowing calls to NetCDF library to use the certificate.

    Args:
        user: User object.
    """
    if not check_certificate(user):
        refresh_certificate(user)

    user_path = os.path.join(settings.WPS_USER_TEMP_PATH, str(user.id))

    if not os.path.exists(user_path):
        os.makedirs(user_path)

        logger.info('Created user directory {}'.format(user_path))

    os.chdir(user_path)

    logger.info('Changed working directory to {}'.format(user_path))

    cert_path = os.path.join(user_path, 'cert.pem')

    with open(cert_path, 'w') as outfile:
        outfile.write(user.auth.cert)

    logger.info('Wrote user certificate')

    dods_cookies_path = os.path.join(user_path, '.dods_cookies')

    if os.path.exists(dods_cookies_path):
        os.remove(dods_cookies_path)

        logger.info('Removed stale dods_cookies file')

    dodsrc_path = os.path.join(user_path, '.dodsrc')

    if not os.path.exists(dodsrc_path):
        with open(dodsrc_path, 'w') as outfile:
            outfile.write('HTTP.COOKIEJAR=.dods_cookies\n')
            outfile.write('HTTP.SSL.CERTIFICATE={}\n'.format(cert_path))
            outfile.write('HTTP.SSL.KEY={}\n'.format(cert_path))
            outfile.write('HTTP.SSL.CAPATH={}\n'.format(settings.WPS_CA_PATH))
            outfile.write('HTTP.SSL.VERIFY=0\n')

        logger.info('Wrote .dodsrc file {}'.format(dodsrc_path))

    return cert_path
