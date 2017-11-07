import datetime
import collections
import json
import re
import random
import string

from django import http
from django import db
from django.core.mail import send_mail
from django.contrib.auth import authenticate
from django.contrib.auth import login
from django.contrib.auth import logout
from django.shortcuts import redirect
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods
from openid.consumer import consumer
from openid.consumer import discover
from openid.consumer.discover import DiscoveryFailure
from openid.extensions import ax
from myproxy.client import MyProxyClient

from wps import forms
from wps import models
from wps import settings
from wps.auth import oauth2
from wps.views import common

logger = common.logger

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

FORGOT_USERNAME_SUBJECT = 'CWT WPS Username Recovery'
FORGOT_USERNAME_MESSAGE = """
Hello {username},

You've requested the recovery of your username: {username}.

Thank you,
ESGF CWT Team
"""

FORGOT_PASSWORD_SUBJECT = 'CWT WPS Password Reset'
FORGOT_PASSWORD_MESSAGE = """
Hello {username},
<br><br>
You've request the reset of you password. Please follow this <a href="{reset_url}">link</a> to reset you password.
<br><br>
Thank you,
ESGF CWT Team
"""

CREATE_SUBJECT = 'Welcome to ESGF compute server'
CREATE_MESSAGE = """
Thank you for creating an account for the ESGF compute server. Please login into your account <a href="{login_url}">here</a>.

If you have any questions or concerns please email the <a href="mailto:{admin_email}">server admin</a>.
"""

def update_user(user, service_type, certs, **extra):
    """ Updates users auth settings """

    if user.auth.api_key == '':
        user.auth.api_key = ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(64))

    user.auth.type = service_type

    user.auth.cert = ''.join(certs)

    user.auth.extra = json.dumps(extra)

    user.auth.save()

    logger.info('Updated auth settings for user {}'.format(user.username))

def openid_find_service_by_type(services, uri):
    for s in services:
        if uri in s.type_uris:
            return s

    return None

def openid_services(openid_url, service_urns):
    requested = collections.OrderedDict()

    try:
        url, services = discover.discoverYadis(openid_url)
    except Exception as e:
        raise Exception('OpenID discovery failed "{}"'.format(e.message))

    for urn in service_urns:
        requested[urn] = openid_find_service_by_type(services, urn)

        if requested[urn] is None:
            raise Exception('OpenID IDP for "{}" does not support service "{}"'.format(openid_url, urn))

    return requested.values()

def openid_begin(request, openid_url):
    try:
        logger.info('Attempting to login user "{}"'.format(openid_url))

        c = consumer.Consumer(request.session, models.DjangoOpenIDStore()) 

        auth_request = c.begin(openid_url)

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

        url = auth_request.redirectURL(settings.OPENID_TRUST_ROOT, settings.OPENID_RETURN_TO)
    except Exception as e:
        raise common.ViewError('Failed to begin OpenID process "{}"'.format(e.message))
    else:
        return url

def openid_complete(request):
    try:
        c = consumer.Consumer(request.session, models.DjangoOpenIDStore())

        response = c.complete(request.GET, settings.OPENID_RETURN_TO)

        if response.status == consumer.CANCEL:
            raise Exception('OpenID authentication cancelled')
        elif response.status == consumer.FAILURE:
            raise Exception('OpenID authentication failed')
        
        openid_url = response.getDisplayIdentifier()

        attrs = handle_openid_attribute_exchange(response)
    except Exception as e:
        raise common.ViewError('Failed to complete OpenID process "{}"'.format(e.message))
    else:
        return openid_url, attrs

def handle_openid_attribute_exchange(response):
    attrs = {'email': None}

    ax_response = ax.FetchResponse.fromSuccessResponse(response)

    if ax_response:
        attrs['email'] = ax_response.get('http://axschema.org/contact/email')[0]

    return attrs

@require_http_methods(['POST'])
@ensure_csrf_cookie
def create(request):
    try:
        form = forms.CreateForm(request.POST)

        data = common.validate_form(form, ('username', 'email', 'openid', 'password'))

        try:
            user = models.User.objects.create_user(data['username'], data['email'], data['password'])
        except db.IntegrityError:
            raise common.DuplicateUserError(username=data['username'])

        models.Auth.objects.create(openid_url=data['openid'], user=user)

        try:
            send_mail(CREATE_SUBJECT,
                      '',
                      settings.ADMIN_EMAIL,
                      [user.email],
                      html_message=CREATE_MESSAGE.format(login_url=settings.LOGIN_URL, admin_email=settings.ADMIN_EMAIL))
        except Exception:
            logger.exception('Error sending confirmation email')
            
            pass
    except Exception as e:
        logger.exception('Error creating account')

        return common.failed(e.message)
    else:
        return common.success('Successfully created account for "{}"'.format(data['username']))

@require_http_methods(['POST'])
@ensure_csrf_cookie
def user_login_openid(request):
    try:
        form = forms.OpenIDForm(request.POST)

        data = common.validate_form(form, ('openid_url',))

        url = openid_begin(request, data['openid_url'])
    except Exception as e:
        logger.exception('Error logging user in with OpenID')

        return common.failed(e.message)
    else:
        return common.success({'redirect': url})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_login_openid_callback(request):
    try:
        openid_url, attrs = openid_complete(request)

        try:
            user = models.User.objects.get(auth__openid_url=openid_url)
        except models.User.DoesNotExist:
            username = openid_url.split('/')[-1]

            user = models.User.objects.create_user(username, attrs['email'])

            models.Auth.objects.create(openid_url=openid_url, user=user)

        login(request, user)
    except Exception as e:
        logger.exception('Error handling OpenID callback')

        return common.failed(e.message)
    else:
        return redirect('{}?expires={}'.format(settings.OPENID_CALLBACK_SUCCESS, request.session.get_expiry_date()))

@require_http_methods(['POST'])
@ensure_csrf_cookie
def user_login(request):
    try:
        form = forms.LoginForm(request.POST)

        data = common.validate_form(form, ('username', 'password'))

        logger.info('Attempting to login user {}'.format(data['username']))

        user = authenticate(request, username=data['username'], password=data['password'])

        if user is None:
            raise common.ViewError('Failed to authenticate user')

        login(request, user)
    except Exception as e:
        logger.exception('Error logging user in')

        return common.failed(e.message)
    else:
        data = common.user_to_json(user)

        data['expires'] = request.session.get_expiry_date()

        return common.success(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_logout(request):
    try:
        common.authentication_required(request)

        logger.info('Logging user {} out'.format(request.user.username))

        logout(request)
    except Exception as e:
        logger.exception('Error logging user out')

        return common.failed(e.message)
    else:
        return common.success('Logged out')

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_oauth2(request):
    try:
        common.authentication_required(request)

        logger.info('Authenticating OAuth2 for {}'.format(request.user.auth.openid_url))

        auth_service, cert_service = openid_services(request.user.auth.openid_url, (URN_AUTHORIZE, URN_RESOURCE))

        redirect_url, state = oauth2.get_authorization_url(auth_service.server_url, cert_service.server_url)

        logger.info('Retrieved authorization url for OpenID {}'.format(request.user.auth.openid_url))

        request.session.update({
            'oauth_state': state,
            'openid': request.user.auth.openid_url
        })
    except Exception as e:
        logger.exception('Error authenticating OAuth2')

        return common.failed(e.message)
    else:
        return common.success({'redirect': redirect_url})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def oauth2_callback(request):
    user = None

    try:
        openid_url = request.session.pop('openid')

        oauth_state = request.session.pop('oauth_state')

        user = models.User.objects.get(auth__openid_url = openid_url)

        token_service, cert_service = openid_services(openid_url, (URN_ACCESS, URN_RESOURCE))

        request_url = '{}?{}'.format(settings.OAUTH2_CALLBACK, request.META['QUERY_STRING'])

        token = oauth2.get_token(token_service.server_url, request_url, oauth_state)

        logger.info('Retrieved OAuth2 token for OpenID {}'.format(oid))

        cert, key, new_token = oauth2.get_certificate(token, token_service.server_url, cert_service.server_url)

        logger.info('Retrieved Certificated for OpenID {}'.format(oid))

        update_user(user, 'oauth2', [cert, key], token=new_token)
    except Exception as e:
        logger.exception('OAuth2 callback failed')

        if user is not None:
            extra = json.loads(user.auth.extra)

            extra['error'] = 'OAuth2 callback failed "{}"'.format(e.message)

            user.auth.extra = json.dumps(extra)

            user.auth.save()

        return redirect(settings.PROFILE_URL)
    else:
        return redirect(settings.PROFILE_URL)

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_mpc(request):
    try:
        common.authentication_required(request)

        form = forms.MPCForm(request.POST)

        data = common.validate_form(form, ('username', 'password'))

        logger.info('Authenticating MyProxyClient for {}'.format(data['username']))

        mpc_service = openid_services(request.user.auth.openid_url, (URN_MPC,))[0]

        try:
            g = re.match('socket://(.*):(.*)', mpc_service.server_url)
        except re.error:
            raise common.ViewError('Failed to parse MyProxyClient host and port')

        if g is None or len(g.groups()) != 2:
            raise Exception('Failed to parse MyProxyClient endpoint')

        host, port = g.groups()

        try:
            m = MyProxyClient(hostname=host, caCertDir=settings.CA_PATH)

            c = m.logon(data['username'], data['password'], bootstrap=True)
        except Exception as e:
            raise common.ViewError('MyProxyClient failed "{}"'.format(e.message))

        logger.info('Authenticated with MyProxyClient backend for user {}'.format(data['username']))

        update_user(request.user, 'myproxyclient', c)
    except Exception as e:
        logger.exception('Error authenticating MyProxyClient')

        return common.failed(e.message)
    else:
        return common.success({
            'type': request.user.auth.type,
            'api_key': request.user.auth.api_key
        })

@require_http_methods(['GET'])
@ensure_csrf_cookie
def forgot_username(request):
    try:
        try:
            email = request.GET['email']
        except KeyError as e:
            raise common.MissingParameterError(parameter=e.message)

        logger.info('Recovering username for "{}"'.format(email))

        try:
            user = models.User.objects.get(email=email)
        except models.User.DoesNotExist:
            raise common.ViewError('No registered user for "{}"'.format(email))

        try:
            send_mail(FORGOT_USERNAME_SUBJECT,
                      FORGOT_USERNAME_MESSAGE.format(username=user.username, login_url=settings.LOGIN_URL),
                      settings.ADMIN_EMAIL,
                      [user.email])
        except:
            raise common.ViewError('Failed to send username recovery email')
    except Exception as e:
        logger.exception('Error recovering username')

        return common.failed(e.message)
    else:
        return common.success({'redirect': settings.LOGIN_URL})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def forgot_password(request):
    try:
        try:
            username = request.GET['username']
        except KeyError as e:
            raise common.MissingParameterError(parameter=e.message)

        logger.info('Starting password reset process for user "{}"'.format(username))

        try:
            user = models.User.objects.get(username=username)
        except models.User.DoesNotExist:
            raise common.ViewError('No registered user "{}"'.format(username))

        try:
            extra = json.loads(user.auth.extra)
        except Exception:
            extra = {}

        extra['reset_token'] = ''.join(random.choice(string.ascii_letters + string.digits) for _ in xrange(64))

        extra['reset_expire'] = datetime.datetime.now() + datetime.timedelta(1)

        user.auth.extra = json.dumps(extra, default=lambda x: x.strftime('%x %X'))

        user.auth.save()

        reset_url = '{}?token={}&username={}'.format(settings.PASSWORD_RESET_URL, extra['reset_token'], user.username)

        try:
            send_mail(FORGOT_PASSWORD_SUBJECT,
                      '',
                      settings.ADMIN_EMAIL,
                      [user.email],
                      html_message=FORGOT_PASSWORD_MESSAGE.format(username=user.username,
                                                              reset_url=reset_url)
                      )
        except:
            raise common.ViewError('Failed to send password recovery email')
    except Exception as e:
        return common.failed(e.message)
    else:
        return common.success({'redirect': settings.LOGIN_URL})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def reset_password(request):
    try:
        try:
            token = str(request.GET['token'])

            username = str(request.GET['username'])

            password = str(request.GET['password'])
        except KeyError as e:
            raise common.MissingParameterError(parameter=e.message)

        logger.info('Resetting password for "{}"'.format(username))

        try:
            user = models.User.objects.get(username=username)
        except models.User.DoesNotExist:
            raise common.ViewError('User "{}" does not exist'.format(username))

        try:
            extra = json.loads(user.auth.extra)
        except Exception:
            extra = {}

        try:
            reset_token = extra['reset_token']

            reset_expire = extra['reset_expire']
        except KeyError as e:
            raise common.ViewError('Invalid reset state, request again')
        finally:
            del extra['reset_token']

            del extra['reset_expire']

        expires = datetime.datetime.strptime(reset_expire, '%x %X')

        if datetime.datetime.now() > expires:
            raise common.ViewError('Reset token has expire, request again')

        if reset_token != token:
            raise common.ViewError('Tokens do not match, request again')

        user.auth.extra = json.dumps(extra)

        user.auth.save()

        user.set_password(password)

        user.save()

        logger.info('Successfully reset password for "{}"'.format(username))
    except Exception as e:
        return common.failed(e.message)
    else:
        return common.success({'redirect': settings.LOGIN_URL})
