#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import json
from datetime import datetime

from celery.utils.log import get_task_logger
from openid.consumer import discover
from OpenSSL import crypto

from wps import models
from wps import processes
from wps.auth import oauth2

logger = get_task_logger('wps.tasks')

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_RESOURCE = 'urn:esg:security:oauth:endpoint:resource'

def openid_find_service_by_type(services, uri):
    for s in services:
        if uri in s.type_uris:
            return s

    return None

@processes.cwt_shared_task()
def check_auth(self, **kwargs):
    self.PUBLISH = processes.RETRY | processes.FAILURE

    self.set_user_creds(**kwargs)

    user_id = kwargs.get('user_id')

    try:
        user = models.User.objects.get(pk=user_id)
    except models.User.DoesNotExist:
        raise Exception('Could not find user')

    cert = crypto.load_certificate(crypto.FILETYPE_PEM, user.auth.cert)

    fmt = '%Y%m%d%H%M%SZ'

    before = datetime.strptime(cert.get_notBefore(), fmt)

    after = datetime.strptime(cert.get_notAfter(), fmt)

    now = datetime.now()

    if (now >= before and now <= after):
        logger.info('Certificate is still valid')

        return

    logger.info('Certificate has expired, renewing')

    if user.auth.type == 'myproxyclient':
        raise Exception('Please relog into MyProxyClient from your user account page.')

    url, services = discover.discoverYadis(user.auth.openid_url)

    auth_service = openid_find_service_by_type(services, URN_AUTHORIZE)

    cert_service = openid_find_service_by_type(services, URN_RESOURCE)

    extra = json.loads(user.auth.extra)

    try:
        token = extra['token']
    except KeyError:
        raise Exception('Missing OAuth2 token, try OAuth2 authentication again')

    cert, key, new_token = oauth2.get_certificate(token, auth_service.server_url, cert_service.server_url)

    logger.info('Recieved new token {}, updating certificate'.format(new_token))

    extra['token'] = new_token

    user.auth.extra = json.dumps(extra)

    user.auth.cert = ''.join([cert, key])

    user.auth.save()
