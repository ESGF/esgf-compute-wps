import logging
import os
import re

from django import http
from django.conf import settings
from django.core.mail import EmailMessage
from django.contrib.auth import login
from django.contrib.auth import logout
from django.db import IntegrityError
from django.shortcuts import redirect
from django.views.decorators.http import require_http_methods
from openid.consumer import discover
from myproxy.client import MyProxyClient
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.exceptions import APIException

from compute_wps import forms
from compute_wps import metrics
from compute_wps import models
from compute_wps.auth import oauth2
from compute_wps.auth import openid
from compute_wps.exceptions import WPSError
from compute_wps.views import common

logger = logging.getLogger('compute_wps.views.auth')

NEW_USER_SUBJ = 'Welcome to LLNL\'s Compute Service'
NEW_USER_MSG = """
Welcome {name},

<pre>
You've successfully register for the ESGF Compute service. You now have access
to ESGF compute resources. To begin start by visiting the <a href="{settings.EXTERNAL_URL}">WPS Service</a> homepage.
You can find some additional resources below.
</pre>

<pre>
<a href="https://github.com/ESGF/esgf-compute-api">ESGF-Compute-API</a>
<a href="https://github.com/ESGF/esgf-compute-api/blob/master/examples/getting_started.ipynb">Getting Started</a>
<a href="https://github.com/ESGF/esgf-compute-api/tree/master/examples">Jupyter Notebooks</a>
</pre>

<pre>
Thank you,
ESGF Compute Team
</pre>
"""

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_ACCESS = 'urn:esg:security:oauth:endpoint:access'
URN_RESOURCE = 'urn:esg:security:oauth:endpoint:resource'
URN_MPC = 'urn:esg:security:myproxy-service'

discover.OpenIDServiceEndpoint.openid_type_uris.extend([
    URN_AUTHORIZE,
    URN_ACCESS,
    URN_RESOURCE,
    URN_MPC
])


def get_oauth2_urls(user):
    logger.info('Retrieving OAuth2 URLs')

    try:
        authorize_url, token_url, certificate_url = user.auth.get('authorize_url', 'token_url', 'certificate_url')
    except KeyError:
        services = openid.services(user.auth.openid_url, (URN_AUTHORIZE, URN_ACCESS, URN_RESOURCE))

        authorize_url = services[0].server_url

        token_url = services[1].server_url

        certificate_url = services[2].server_url

        user.auth.update(authorize_url=authorize_url, token_url=token_url, certificate_url=certificate_url)

    return authorize_url, token_url, certificate_url


class InternalUserViewSet(viewsets.GenericViewSet):
    @action(detail=True)
    def details(self, request, pk):
        try:
            user = models.User.objects.get(pk=pk)
        except models.User.DoesNotExist:
            raise APIException('User does not exist')

        data = {
            'first_name': user.first_name,
            'email': user.email,
        }

        return Response(data, status=200)

    @action(detail=True)
    def certificate(self, request, pk):
        try:
            user = models.User.objects.get(pk=pk)
        except models.User.DoesNotExist:
            raise APIException('User does not exist')

        try:
            _, token_url, certificate_url = get_oauth2_urls(user)

            certs = oauth2.get_certificate(user, token_url, certificate_url)
        except Exception as e:
            logger.exception('Error retrieving user client certificate')

            raise APIException(str(e))

        data = {
            'certificate': certs
        }

        return Response(data, status=200)


class MPCEndpointParseError(WPSError):
    def __init__(self):
        msg = 'Parsing host/port from OpenID services failed'

        super(MPCEndpointParseError, self).__init__(msg)


def send_welcome_mail(user):
    if user.first_name is None:
        name = user.username
    else:
        name = user.get_full_name()

    msg = NEW_USER_MSG.format(user=user, name=name, settings=settings)

    email = EmailMessage(NEW_USER_SUBJ, msg, settings.ADMIN_EMAIL,
                         to=[user.email, ], bcc=[settings.ADMIN_EMAIL, ])

    email.content_subtype = 'html'

    email.send(fail_silently=True)


@require_http_methods(['GET'])
def authorization(request):
    if not request.user.is_authenticated:
        try:
            proto = request.META['HTTP_X_FORWARDED_PROTO']

            host = request.META['HTTP_X_FORWARDED_HOST']

            uri = request.META['HTTP_X_FORWARDED_URI'].strip('/')
        except KeyError as e:
            raise WPSError('Could not reconstruct forwarded url, missing {!s}', e)

        prefix = request.META.get('HTTP_X_FORWARDED_PREFIX', '').strip('/')

        logger.info('PROTO %r', proto)
        logger.info('HOST %r', host)
        logger.info('URI %r', uri)
        logger.info('PREFIX %r', prefix)

        forward = '{!s}://{!s}'.format(proto, host)

        if uri != '':
            forward = '{!s}/{!s}'.format(forward, uri)

        if prefix != '':
            forward = '{!s}/{!s}'.format(forward, prefix)

        redirect_url = '{!s}?next={!s}'.format(settings.LOGIN_URL, forward)

        return http.HttpResponseRedirect(redirect_url)

    return http.HttpResponse()


@require_http_methods(['POST'])
def user_login_openid(request):
    try:
        form = forms.OpenIDForm(request.POST)

        data = common.validate_form(form, ('openid_url', 'next', 'response'))

        url = openid.begin(request, **data)
    except WPSError as e:
        logger.exception('Error logging user in with OpenID')

        return common.failed(str(e))
    else:
        return common.success({'redirect': url})
    finally:
        if 'openid_url' in request.POST:
            metrics.track_login(metrics.WPS_OPENID_LOGIN,
                                request.POST['openid_url'])


def add_new_user(openid_url, attrs):
    username = openid_url.split('/')[-1]

    first = attrs.get('first', None)

    last = attrs.get('last', None)

    try:
        user = models.User.objects.create_user(username, attrs['email'], first_name=first, last_name=last)
    except IntegrityError:
        raise WPSError('Username {!r} already exists', username)

    models.Auth.objects.create(openid_url=openid_url, user=user)

    user.auth.generate_api_key()

    send_welcome_mail(user)

    return user


@require_http_methods(['GET'])
def user_login_openid_callback(request):
    try:
        openid_url, attrs = openid.complete(request)

        try:
            user = models.User.objects.get(auth__openid_url=openid_url)
        except models.User.DoesNotExist:
            user = add_new_user(openid_url, attrs)

        login(request, user)

        next = request.GET.get('next', None)

        response = request.GET.get('response', 'redirect')
    except WPSError as e:
        logger.exception('Error handling OpenID callback')

        redirect_url = '{!s}/auth/login?error={!s}'.format(settings.EXTERNAL_URL, str(e))

        if response == 'redirect':
            return redirect(redirect_url)
        elif resposne == 'json':
            return http.JsonResponse({'error': str(e)})
        else:
            return http.HttpResponseBadRequest('Unknown response type {!s}'.format(response))
    else:
        metrics.track_login(metrics.WPS_OPENID_LOGIN_SUCCESS,
                            user.auth.openid_url)

        if response == 'redirect':
            redirect_url = '{!s}?expires={!s}'.format(settings.OPENID_CALLBACK_SUCCESS_URL,
                                                      request.session.get_expiry_date())

            if next is not None and next != 'null' and next != '':
                redirect_url = '{!s}&next={!s}'.format(redirect_url, next)

            return redirect(redirect_url)
        elif response == 'json':
            return http.JsonResponse({'expires': request.session.get_expiry_date(),
                                      'token': user.auth.api_key})
        else:
            return http.HttpResponseBadRequest('Unknown response type {!s}'.format(response))


@require_http_methods(['GET'])
def user_logout(request):
    try:
        common.authentication_required(request)

        logger.info('Logging user {} out'.format(request.user.username))

        logout(request)
    except WPSError as e:
        logger.exception('Error logging user out')

        return common.failed(str(e))
    except Exception as e:
        logger.exception('Error logging user out')

        return common.failed(str(e))
    else:
        return common.success('Logged out')


@require_http_methods(['POST'])
def login_oauth2(request):
    try:
        common.authentication_required(request)

        logger.info('OAuth2 authorization for {!s}'.format(request.user.auth.openid_url))

        if 'oauth_state' in request.session:
            del request.session['oauth_state']

            logger.info('Removing OAuth2 state')

        authorize_url, _, certificate_url = get_oauth2_urls(request.user)

        redirect_url, state = oauth2.get_authorization_url(authorize_url, certificate_url)

        logger.info('Redirecting to %r setting state to %r', redirect_url, state)

        request.session.update({
            'oauth_state': state,
            'openid': request.user.auth.openid_url
        })
    except WPSError as e:
        logger.exception('Error authenticating OAuth2')

        return common.failed(str(e))
    else:
        return common.success({'redirect': redirect_url})
    finally:
        # Not tracking anonymous
        if not request.user.is_anonymous:
            metrics.track_login(metrics.WPS_OAUTH_LOGIN, request.user.auth.openid_url)


@require_http_methods(['GET'])
def oauth2_callback(request):
    user = None

    try:
        openid_url = request.session['openid']

        oauth_state = request.session['oauth_state']

        logger.info('Handling OAuth2 callback for %r current state %r', openid_url, oauth_state)

        user = models.User.objects.get(auth__openid_url=openid_url)

        _, token_url, certificate_url = get_oauth2_urls(user)

        request_url = request.build_absolute_uri()

        token = oauth2.get_token(token_url, request_url, oauth_state)

        logger.info('Fetched token %r', token)

        user.auth.update(type=models.OAUTH2_TYPE, token=token, state=oauth_state)

        _ = oauth2.get_certificate(user, token_url, certificate_url)
    except KeyError as e:
        logger.exception('Missing %r key from session data', e)

        return common.failed('Invalid OAuth state, report to server administrator')
    except (WPSError, oauth2.OAuth2Error) as e:
        logger.exception('OAuth2 callback failed')

        if user is not None:
            user.auth.update(error='OAuth2 callback failed "{}"'.format(str(e)))

    logger.info('Finished handling OAuth2 callback, redirect to profile')

    metrics.track_login(metrics.WPS_OAUTH_LOGIN_SUCCESS, user.auth.openid_url)

    return redirect(settings.PROFILE_URL)


@require_http_methods(['GET'])
def user_cert(request):
    try:
        metrics.WPS_CERT_DOWNLOAD.inc()

        common.authentication_required(request)

        _, token_url, certificate_url = get_oauth2_urls(request.user)

        certs = oauth2.get_certificate(request.user, token_url, certificate_url)

        content_type = 'application/force-download'

        response = http.HttpResponse(certs, content_type=content_type)

        response['Content-Disposition'] = 'attachment; filename="cert.pem"'

        response['Content-Length'] = len(certs)
    except WPSError:
        logger.exception('Error retrieving user client certificate')

        return http.HttpResponseBadRequest()
    except Exception as e:
        logger.exception('Error retrieving user client certificate')

        return http.HttpResponseBadRequest(str(e))
    else:
        return response


@require_http_methods(['POST'])
def login_mpc(request):
    try:
        common.authentication_required(request)

        form = forms.MPCForm(request.POST)

        data = common.validate_form(form, ('username', 'password'))

        logger.info('Authenticating MyProxyClient for {}'.format(data['username']))

        services = openid.services(request.user.auth.openid_url, (URN_MPC,))

        g = re.match('socket://(.*):(.*)', services[0].server_url)

        if g is None or len(g.groups()) != 2:
            raise MPCEndpointParseError()

        host, port = g.groups()

        from OpenSSL import SSL

        MyProxyClient.SSL_METHOD = SSL.TLSv1_2_METHOD

        try:
            m = MyProxyClient(hostname=host, caCertDir=settings.CA_PATH)

            c = m.logon(data['username'], data['password'], bootstrap=True)
        except Exception:
            raise common.AuthenticationError(user=data['username'])

        logger.info('Authenticated with MyProxyClient backend for user {}'.format(data['username']))

        request.user.auth.update(type=models.MPC_TYPE, certs=c)
    except WPSError as e:
        logger.exception('Error authenticating MyProxyClient')

        return common.failed(str(e))
    else:
        metrics.track_login(metrics.WPS_MPC_LOGIN_SUCCESS, request.user.auth.openid_url)

        return common.success({
            'type': request.user.auth.type,
            'api_key': request.user.auth.api_key
        })
    finally:
        if not request.user.is_anonymous:
            metrics.track_login(metrics.WPS_MPC_LOGIN, request.user.auth.openid_url)
