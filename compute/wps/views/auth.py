import datetime
import json
import re
import random
import string

from django import http
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
from myproxy.client import MyProxyClient

from wps import forms
from wps import models
from wps import settings
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

def openid_find_service_by_type(services, uri):
    for s in services:
        if uri in s.type_uris:
            return s

    return None

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

@require_http_methods(['POST'])
@ensure_csrf_cookie
def create(request):
    try:
        form = forms.CreateForm(request.POST)

        if not form.is_valid():
            raise Exception(form.errors)

        username = form.cleaned_data['username']

        email = form.cleaned_data['email']

        openid = form.cleaned_data['openid']

        password = form.cleaned_data['password']

        try:
            user = models.User.objects.create_user(username, email, password)
        except IntegrityError:
            raise Exception('User already exists')

        models.Auth.objects.create(openid_url=openid, user=user)

        try:
            send_mail(CREATE_SUBJECT,
                      '',
                      settings.ADMIN_EMAIL,
                      [user.email],
                      html_message=CREATE_MESSAGE.format(login_url=settings.LOGIN_URL, admin_email=settings.ADMIN_EMAIL))
        except Exception:
            logger.exception('Error sending user account creation notice')
    except Exception as e:
        logger.exception('Error creating account')

        return common.failed(e.message)
    else:
        return common.success('Successfully created account for "{}"'.format(username))

@require_http_methods(['POST'])
@ensure_csrf_cookie
def user_login_openid(request):
    try:
        form = forms.OpenIDForm(request.POST)

        if not form.is_valid():
            raise Exception(form.errors)

        openid_url = form.cleaned_data['openid_url']

        logger.info('Attempting to login user "{}"'.format(openid_url))

        c = consumer.Consumer(request.session, models.DjangoOpenIDStore()) 

        try:
            auth_request = c.begin(openid_url)
        except DiscoveryFailure as e:
            logger.exception('OpenID discovery error')

            raise Exception('OpenID discovery error')

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
        logger.exception('Error logging user in with OpenID')

        return common.failed(e.message)
    else:
        return common.success({'redirect': url})

def __handle_openid_attribute_exchange(response):
    attrs = {
        'email': None
    }

    ax_response = ax.FetchResponse.fromSuccessResponse(response)

    if ax_response:
        attrs['email'] = ax_response.get('http://axschema.org/contact/email')[0]

    return attrs

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_login_openid_callback(request):
    try:
        c = consumer.Consumer(request.session, models.DjangoOpenIDStore())

        try:
            response = c.complete(request.GET, settings.OPENID_RETURN_TO)
        except:
            raise Exception('Failed to complete OpenID')

        if response.status == consumer.CANCEL:
            raise Exception('OpenID authentication cancelled')
        elif response.status == consumer.FAILURE:
            raise Exception('OpenID authentication failed')
        
        openid_url = response.getDisplayIdentifier()

        attrs = __handle_openid_attribute_exchange(response)

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

        if not form.is_valid():
            raise Exception(form.errors)

        username = form.cleaned_data['username']

        password = form.cleaned_data['password']

        logger.info('Attempting to login user {}'.format(username))

        user = authenticate(request, username=username, password=password)

        if user is not None:
            logger.info('Authenticate user {}, logging in'.format(username))

            login(request, user)
        else:
            raise Exception('Authentication failed')
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

def update_user(user, service_type, certs, **extra):
    """ Updates users auth settings """

    if user.auth.api_key == '':
        user.auth.api_key = ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(64))

    user.auth.type = service_type

    user.auth.cert = ''.join(certs)

    user.auth.extra = json.dumps(extra)

    user.auth.save()

    logger.info('Updated auth settings for user {}'.format(user.username))

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_oauth2(request):
    try:
        common.authentication_required(request)

        logger.info('Authenticating OAuth2 for {}'.format(request.user.auth.openid_url))

        try:
            url, services = discover.discoverYadis(request.user.auth.openid_url)
        except Exception:
            raise Exception('Failed to retrieve OpenID')

        auth_service = openid_find_service_by_type(services, URN_AUTHORIZE)

        cert_service = openid_find_service_by_type(services, URN_RESOURCE)

        redirect_url, state = oauth2.get_authorization_url(auth_service.server_url, cert_service.server_url)

        logger.info('Retrieved authorization url for OpenID {}'.format(oid_url))

        session = {
            'oauth_state': state,
            'openid': oid_url,
        }

        request.session.update(session)
    except Exception as e:
        logger.exception('Error authenticating OAuth2')

        return common.failed(e.message)
    else:
        return common.success({'redirect': redirect_url})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def oauth2_callback(request):
    try:
        oid = request.session.pop('openid')

        oauth_state = request.session.pop('oauth_state')
    except KeyError as e:
        logger.exception('Invalid session state')

        return redirect(settings.LOGIN_URL)

    try:
        url, services = discover.discoverYadis(oid)
    except Exception:
        raise Exception('Failed to retrieve OpenID')

    token_service = openid_find_service_by_type(services, URN_ACCESS)

    cert_service = openid_find_service_by_type(services, URN_RESOURCE)

    request_url = '{}?{}'.format(settings.OAUTH2_CALLBACK, request.META['QUERY_STRING'])

    token = oauth2.get_token(token_service.server_url, request_url, oauth_state)

    logger.info('Retrieved OAuth2 token for OpenID {}'.format(oid))

    cert, key, new_token = oauth2.get_certificate(token, token_service.server_url, cert_service.server_url)

    logger.info('Retrieved Certificated for OpenID {}'.format(oid))

    try:
        user = models.User.objects.get(auth__openid_url = oid)
    except:
        raise Exception('User does not exist for "{}"'.format(oid))

    update_user(user, 'oauth2', [cert, key], token=new_token)

    return redirect(settings.PROFILE_URL)

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_mpc(request):
    try:
        common.authentication_required(request)

        form = forms.MPCForm(request.POST)

        if not form.is_valid():
            raise Exception(form.errors)

        username = form.cleaned_data['username']

        password = form.cleaned_data['password']

        logger.info('Authenticating MyProxyClient for {}'.format(username))

        try:
            url, services = discover.discoverYadis(request.user.auth.openid_url)
        except Exception:
            raise Exception('Failed to retrieve OpenID')

        mpc_service = openid_find_service_by_type(services, URN_MPC)

        g = re.match('socket://(.*):(.*)', mpc_service.server_url)

        if g is None:
            raise Exception('Failed to parse MyProxyClient endpoint')

        host, port = g.groups()

        m = MyProxyClient(hostname=host, caCertDir=settings.CA_PATH)

        c = m.logon(username, password, bootstrap=True)

        logger.info('Authenticated with MyProxyClient backend for user {}'.format(username))

        update_user(request.user, 'myproxyclient', c)

        data = {
            'type': request.user.auth.type,
            'api_key': request.user.auth.api_key
        }
    except Exception as e:
        logger.exception('Error authenticating MyProxyClient')

        return common.failed(e.message)
    else:
        return common.success(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def forgot_username(request):
    try:
        if 'email' not in request.GET:
            raise Exception('Please provide the email address of the account')

        logger.info('Recovering username for "{}"'.format(request.GET['email']))

        user = models.User.objects.get(email=request.GET['email'])

        try:
            send_mail(FORGOT_USERNAME_SUBJECT,
                      FORGOT_USERNAME_MESSAGE.format(username=user.username, login_url=settings.LOGIN_URL),
                      settings.ADMIN_EMAIL,
                      [user.email])
        except:
            raise Exception('Failed sending reset link to accounts email')
    except models.User.DoesNotExist:
        logger.exception('User does not exist with email "{}"'.format(request.GET['email']))

        return common.failed('There is not user associated with the email "{}"'.format(request.GET['email']))
    except Exception as e:
        logger.exception('Error recovering username')

        return common.failed(e.message)
    else:
        return common.success({'redirect': settings.LOGIN_URL})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def forgot_password(request):
    try:
        if 'username' not in request.GET:
            raise Exception('Please provide the username you are trying to reset the password for')

        username = request.GET['username']

        logger.info('Starting password reset process for user "{}"'.format(username))

        user = models.User.objects.get(username=username)

        extra = json.loads(user.auth.extra)

        token = extra['reset_token'] = ''.join(random.choice(string.ascii_letters + string.digits) for _ in xrange(64))

        extra['reset_expire'] = datetime.datetime.now() + datetime.timedelta(1)

        user.auth.extra = json.dumps(extra, default=lambda x: x.strftime('%x %X'))

        user.auth.save()

        reset_url = '{}?token={}&username={}'.format(settings.PASSWORD_RESET_URL, token, user.username)

        try:
            send_mail(FORGOT_PASSWORD_SUBJECT,
                      '',
                      settings.ADMIN_EMAIL,
                      [user.email],
                      html_message=FORGOT_PASSWORD_MESSAGE.format(username=user.username,
                                                              reset_url=reset_url)
                      )
        except:
            raise Exception('Error sending reset password email')
    except models.User.DoesNotExist:
        return common.failed('Username "{}" does not exist'.format(request.GET['username']))
    except Exception as e:
        return common.failed(e.message)
    else:
        return common.success({'redirect': settings.LOGIN_URL})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def reset_password(request):
    try:
        token = str(request.GET['token'])

        username = str(request.GET['username'])

        password = str(request.GET['password'])

        logger.info('Resetting password for "{}"'.format(username))

        user = models.User.objects.get(username=username)

        extra = json.loads(user.auth.extra)

        if 'reset_token' not in extra or 'reset_expire' not in extra:
            raise Exception('Invalid reset state')

        expires = datetime.datetime.strptime(extra['reset_expire'], '%x %X')

        if datetime.datetime.now() > expires:
            raise Exception('Reset token has expired')

        if extra['reset_token'] != token:
            raise Exception('Tokens do not match')

        del extra['reset_token']

        del extra['reset_expire']

        user.auth.extra = json.dumps(extra)

        user.auth.save()

        user.set_password(password)

        user.save()

        logger.info('Successfully reset password for "{}"'.format(username))
    except KeyError as e:
        logger.exception('Missing key {}'.format(e))

        return common.failed('Missing required parameter {}'.format(e))
    except models.User.DoesNotExist:
        logger.exception('User does not exist with username "{}"'.format(request.GET['username']))

        return common.failed('Username "{}" does not exist'.format(request.GET['username']))
    except Exception as e:
        logger.exception('Error resetting password for user "{}"'.format(request.GET['username']))

        return common.failed(e.message)
    else:
        return common.success({'redirect': settings.LOGIN_URL})
