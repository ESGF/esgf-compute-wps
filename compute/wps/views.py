import json
import logging
import re

import django
from django import http
from django.contrib.auth import authenticate
from django.contrib.auth import login as dlogin
from django.contrib.auth import logout
from django.core import serializers
from django.shortcuts import render
from django.shortcuts import redirect
from django.shortcuts import get_list_or_404
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods
from django.views.static import serve
from cwt.wps_lib import metadata

from wps import forms
from wps import models
from wps import node_manager
from wps import settings
from wps.auth import openid
from wps.auth import oauth2

logger = logging.getLogger('wps.views')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def oauth2_callback(request):
    logger.info('OAuth2 callback')

    try:
        oid = request.session.pop('openid')

        oid_response = request.session.pop('openid_response')

        oauth_state = request.session.pop('oauth_state')
    except KeyError as e:
        logger.debug('Session did not contain key "%s"', e.message)

        return redirect('login')

    manager = node_manager.NodeManager()

    manager.auth_oauth2_callback(oid, oid_response, request.META['QUERY_STRING'], oauth_state)

    return redirect('home')

@require_http_methods(['POST'])
@ensure_csrf_cookie
def create(request):
    form = forms.CreateForm(request.POST)

    if not form.is_valid():
        logger.info('Form is not valid')

        return http.JsonResponse({ 'status': 'failure', 'errors': form.errors })

    username = form.cleaned_data['username']

    email = form.cleaned_data['email']

    openid = form.cleaned_data['openid']

    password = form.cleaned_data['password']

    logger.info('Creating new account for {}'.format(username))

    user = models.User.objects.create_user(username, email, password)

    user.save()

    user.auth = models.Auth(openid_url=openid)

    user.auth.save()

    return http.JsonResponse({ 'status': 'success' })

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def login(request):
    if request.method == 'POST':
        form = forms.LoginForm(request.POST)

        if form.is_valid():
            username = form.cleaned_data['username']

            password = form.cleaned_data['password']

            logger.info('Attempting to login user {}'.format(username))

            user = authenticate(request, username=username, password=password)

            if user is not None:
                logger.info('Authenticate user {}, logging in'.format(username))

                dlogin(request, user)

                return http.JsonResponse({ 'status': 'success' })
            else:
                logger.warning('Failed to authenticate user')

                return http.JsonResponse({ 'status': 'failure', 'errors': 'bad login' })
        else:
            logger.info('Login form is invalid')

            return http.JsonResponse({ 'status': 'failure', 'errors': form.errors })

    return render(request, 'wps/login.html')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def logout_view(request):
    if request.user.is_authenticated():
        logger.info('Logging user {} out'.format(request.user.username))

        logout(request)

        return http.JsonResponse({'status': 'success'})

    return http.JsonResponse({'status': 'failed', 'errors': 'User not authenticated'})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_oauth2(request):
    if request.user.is_authenticated():
        form = forms.OpenIDForm(request.POST)

        if form.is_valid():
            oid_url = form.cleaned_data.get('openid')

            logger.info('Authenticating OAuth2 for {}'.format(oid_url))

            manager = node_manager.NodeManager()

            redirect_url, session = manager.auth_oauth2(oid_url)

            request.session.update(session)

            return http.JsonResponse({'status': 'success', 'redirect': redirect_url})
        else:
            logger.warning('OAuth2 login form is invalid')

            errors = form.errors
    else:
        logger.warning('User is not authenticated')

        errors = 'User not authenticated'

    return http.JsonResponse({'status': 'failed', 'errors': errors})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_mpc(request):
    if request.user.is_authenticated():
        form = forms.MPCForm(request.POST)

        if form.is_valid():
            oid_url = form.cleaned_data.get('openid')

            username = form.cleaned_data.get('username')

            password = form.cleaned_data.get('password')

            logger.info('Authenticating MyProxyClient for {}'.format(oid_url))

            manager = node_manager.NodeManager()

            try:
                api_key = manager.auth_mpc(oid_url, username, password)
            except Exception as e:
                return http.JsonResponse({'status': 'failed', 'errors': e.message})

            return http.JsonResponse({'status': 'success'})
        else:
            logger.warning('MyProxyClient login form is invalid')

            errors = form.errors
    else:
        logger.warning('User is not authenticated')

        errors = 'User not authenticated'

    return http.JsonResponse({'status': 'failed', 'errors': errors})

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps(request):
    manager = node_manager.NodeManager()

    try:
        api_key, op, identifier, data_inputs = manager.handle_request(request)

        logger.info('Handling WPS request {} for api key {}'.format(op, api_key))

        try:
            user = models.User.objects.filter(auth__api_key=api_key)[0]
        except IndexError:
            logger.exception('Unable to find user with api key {}'.format(api_key))

            raise Exception('Unable to find a user with the api key {}'.format(api_key))

        if op == 'getcapabilities':
            response = manager.get_capabilities()
        elif op == 'describeprocess':
            response = manager.describe_process(identifier)
        else:
            response = manager.execute(user, identifier, data_inputs)
    except node_manager.NodeManagerWPSError as e:
        logger.exception('Specific WPS error')

        response = e.exc_report.xml()
    except django.db.ProgrammingError:
        logger.exception('Handling Django exception')

        exc_report = metadata.ExceptionReport(settings.VERSION)

        exc_report.add_exception(metadata.NoApplicableCode, 'Database has not been initialized')

        response = exc_report.xml()
    except Exception as e:
        logger.exception('Handling WPS general exception')

        exc_report = metadata.ExceptionReport(settings.VERSION)

        exc_report.add_exception(metadata.NoApplicableCode, e.message)

        response = exc_report.xml()

    return http.HttpResponse(response, content_type='text/xml')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def regen_capabilities(request):
    manager = node_manager.NodeManager()

    manager.generate_capabilities()

    return http.HttpResponse('Regenerated capabilities')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def cdas2_capabilities(request):
    manager = node_manager.NodeManager()

    manager.cdas2_capabilities()

    return http.HttpResponse('CDAS capabilities')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def status(request, job_id):
    manager = node_manager.NodeManager()

    status = manager.get_status(job_id)

    return http.HttpResponse(status, content_type='text/xml')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def servers(request):
    servers = models.Server.objects.all()

    data = dict((x.id, {
                        'host': x.host,
                        'added': x.added_date,
                        'status': x.status,
                        'capabilities': x.capabilities
                       }) for x in servers)

    return http.JsonResponse(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def processes(request, server_id):
    try:
        server = models.Server.objects.get(pk=server_id)
    except models.Server.DoesNotExist as e:
        raise http.JsonResponse({'status': 'failed', 'errors': e.message})

    data = dict((x.id, {
                        'identifier': x.identifier,
                        'backend': x.backend,
                        'description': x.description,
                       }) for x in server.processes.all())

    return http.JsonResponse(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def regenerate_api_key(request, user_id):
    if not request.user.is_authenticated():
        return http.JsonResponse({'status': 'failed', 'errors': 'User not logged in.'})

    manager = node_manager.NodeManager()

    api_key = manager.regenerate_api_key(user_id)

    return http.JsonResponse({'api_key': api_key})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user(request):
    if not request.user.is_authenticated():
        return http.JsonResponse({'status': 'failed', 'errors': 'User not logged in.'})

    data = {
            'id': request.user.id,
            'username': request.user.username,
            'email': request.user.email,
           }

    if request.user.auth is not None:
        if request.user.auth.openid == '':
            oid = openid.OpenID.retrieve_and_parse(request.user.auth.openid_url)

            request.user.auth.openid = oid.response

            request.user.auth.save()

        data['openid'] = request.user.auth.openid_url
        data['type'] = request.user.auth.type
        data['api_key'] = request.user.auth.api_key

    return http.JsonResponse(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def jobs(request, user_id):
    if not request.user.is_authenticated():
        return http.JsonResponse({'status': 'failed', 'errors': 'User not logged in.'})

    try:
        user = models.User.objects.get(pk=user_id)
    except models.User.DoesNotExist as e:
        return http.JsonResponse({'status': 'failed', 'errors': e.message})

    jobs = [{
             'id': x.id,
             'server': x.server.id,
             'elapsed': x.elapsed,
             'status': [{
                         'id': y.id,
                         'created': y.created_date,
                         'status': y.status,
                         'messages': [{
                                       'id': z.id,
                                       'created': z.created_date,
                                       'message': z.message,
                                       'percent': z.percent,
                                       'exception': z.exception,
                                      } for z in y.message_set.all()]
                        } for y in x.status_set.all()]
            } for x in user.job_set.all()]

    return http.JsonResponse(dict(jobs=jobs))

@ensure_csrf_cookie
def output(request, file_name):
    return serve(request, file_name, document_root=settings.OUTPUT_LOCAL_PATH)

@ensure_csrf_cookie
def home(request):
    return render(request, 'wps/debug.html')
