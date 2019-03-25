from __future__ import absolute_import

import collections
import logging

import requests
from django.conf import settings
from openid.consumer import consumer
from openid.consumer import discover
from openid.extensions import ax
from openid.yadis import manager

from wps import models
from wps import WPSError

logger = logging.getLogger('wps.auth.openid')

class DiscoverError(WPSError):
    def __init__(self, url, error):
        msg = 'Discovery of OpenID services from "{url}" has failed'

        super(DiscoverError, self).__init__(msg, url=url, error=error)

class ServiceError(WPSError):
    def __init__(self, url, urn):
        msg = 'Service "{urn}" is not supported by "{url}"'

        super(ServiceError, self).__init__(msg, url=url, urn=urn)

class AuthenticationCancelError(WPSError):
    def __init__(self, response):
        msg = 'Authentication cancel for "{url}"'

        super(AuthenticationCancelError, self).__init__(msg, url=response.identity_url)

class AuthenticationFailureError(WPSError):
    def __init__(self, response):
        msg = 'Authentication failure for "{url}" with message "{error}"'

        super(AuthenticationFailureError, self).__init__(msg, url=response.identity_url, error=response.message)

class MissingAttributeError(WPSError):
    def __init__(self, name):
        msg = 'Attribute "{name}" could not be retrieved from OpenID AX extension'

        super(MissingAttributeError, self).__init__(msg, name=name)

def find_service_by_type(services, uri):
    for s in services:
        if uri in s.type_uris:
            return s

    return None

def services(openid_url, service_urns):
    try:
        requests.get(openid_url, timeout=(2, 20))
    except requests.ConnectTimeout:
        raise DiscoverError(openid_url, 'Timed out connecting to'
                             ' {!r}'.format(openid_url))
    except requests.ReadTimeout:
        raise DiscoverError(openid_url, 'Timed out reading from'
                             ' {!r}'.format(openid_url))
    except Exception as e:
        raise DiscoverError(openid_url, 'Error contacting OpenID service:'
                             ' {!r}'.format(e))

    requested = collections.OrderedDict()

    try:
        url, services = discover.discoverYadis(openid_url)
    except discover.DiscoveryFailure as e:
        raise DiscoverError(openid_url, e)

    for urn in service_urns:
        service = find_service_by_type(services, urn)

        if service is None:
            raise ServiceError(openid_url, urn)

        requested[urn] = service

    return list(requested.values())

def begin(request, openid_url, next):
    disc = manager.Discovery(request.session, openid_url)

    # Clean up any residual data from pevious attempts
    disc.cleanup(force=True)

    service = disc.getNextService(discover.discover)

    c = consumer.Consumer(request.session, models.DjangoOpenIDStore()) 

    try:
        auth_request = c.beginWithoutDiscovery(service)
    except consumer.DiscoveryFailure as e:
        raise DiscoverError(openid_url, e)

    fetch_request = ax.FetchRequest()

    attributes = [
        ('http://axschema.org/contact/email', 'email'),
        ('http://axschema.org/namePerson', 'fullname'),
        ('http://axschema.org/namePerson/first', 'firstname'),
        ('http://axschema.org/namePerson/last', 'lastname'),
        ('http://axschema.org/namePerson/friendly', 'nickname'),
        ('http://schema.openid.net/contact/email', 'old_email'),
        ('http://schema.openid.net/namePerson', 'old_fullname'),
        ('http://schema.openid.net/namePerson/friendly', 'old_nickname')
    ]

    for attr, alias in attributes:
        fetch_request.add(ax.AttrInfo(attr, alias=alias, required=True))

    auth_request.addExtension(fetch_request)

    return_to = settings.WPS_OPENID_RETURN_TO

    if next is not None:
        return_to = '{!s}?next={!s}'.format(return_to, next)

    url = auth_request.redirectURL(settings.WPS_OPENID_TRUST_ROOT, return_to)

    return url

def complete(request):
    c = consumer.Consumer(request.session, models.DjangoOpenIDStore())

    response = c.complete(request.GET, settings.WPS_OPENID_RETURN_TO)

    if response.status == consumer.CANCEL:
        raise AuthenticationCancelError(response)
    elif response.status == consumer.FAILURE:
        raise AuthenticationFailureError(response)

    openid_url = response.getDisplayIdentifier()

    attrs = handle_attribute_exchange(response)

    return openid_url, attrs

def handle_attribute_exchange(response):
    attributes = {
        'email': 'http://axschema.org/contact/email',
        'first': 'http://axschema.org/namePerson/first',
        'last': 'http://axschema.org/namePerson/last',
    }

    ax_response = ax.FetchResponse.fromSuccessResponse(response)

    attrs = {}

    if ax_response is not None:
        for key, value in list(attributes.items()):
            try:
                attrs[key] = ax_response.get(value)[0]
            except (KeyError, IndexError):
                raise MissingAttributeError(key)

        # Need a minimum of email to create a new account
        if 'email' not in attrs:
            raise MissingAttributeError('email')

    return attrs

