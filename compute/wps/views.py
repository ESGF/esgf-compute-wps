import json
import logging

import django
from django import http
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
from wps import tasks
from wps.auth import openid
from wps.auth import oauth2

logger = logging.getLogger(__name__)

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_ACCESS = 'urn:esg:security:oauth:endpoint:access'
URN_RESOURCE = 'urn:esg:security:oauth:endpoint:resource'

@require_http_methods(['GET'])
def oauth2_callback(request):
    try:
        openid_url = request.session.pop('openid')

        openid_response = request.session.pop('openid_response')

        oauth_state = request.session.pop('oauth_state')
    except KeyError as e:
        logger.debug('Session did not contain key "%s"', e.message)

        return redirect('login')

    request_url = '{0}?{1}'.format(settings.OAUTH2_CALLBACK,
            request.META['QUERY_STRING'])

    oid = openid.OpenID.parse(openid_response)

    token_service = oid.find(URN_ACCESS)

    try:
        token = oauth2.get_token(token_service.uri, request_url, oauth_state) 
    except oauth2.OAuth2Error:
        return http.HttpResponseBadRequest('OAuth2 callback was not passed the correct parameters')

    manager = node_manager.NodeManager()

    api_key = manager.create_user(openid_url, openid_response, token)

    return http.HttpResponse('Your new api key: {}'.format(api_key))

@require_http_methods(['GET', 'POST'])
def oauth2_login(request):
    if request.method == 'POST':
        form = forms.OpenIDForm(request.POST)

        if form.is_valid():
            openid_url = form.cleaned_data['openid']

            try:
                oid = openid.OpenID.retrieve_and_parse(openid_url)

                auth_service = oid.find(URN_AUTHORIZE)

                cert_service = oid.find(URN_RESOURCE)
            except openid.OpenIDError:
                return http.HttpResponseBadRequest('Unable to retrieve authorization and certificate urls from OpenID metadata')

            try:
                auth_url, state = oauth2.get_authorization_url(auth_service.uri, cert_service.uri)
            except oauth2.OAuth2Error:
                return http.HttpResponseBadRequest('Could not retrieve the OAuth2 authorization url')

            request.session['oauth_state'] = state

            request.session['openid'] = openid_url

            request.session['openid_response'] = oid.response 

            return redirect(auth_url)
    else:
        form = forms.OpenIDForm()

    return render(request, 'wps/login.html', { 'form': form })

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps(request):
    manager = node_manager.NodeManager()

    try:
        api_key, op, identifier, data_inputs = manager.handle_request(request)

        logger.info('Transformed WPS request Operation: %s Identifier: %s '
                'DataInputs: %s', op, identifier, data_inputs)

        try:
            user = models.User.objects.filter(oauth2__api_key=api_key)[0]
        except IndexError:
            # Always want to check for a user, just not error in DEBUG mode
            if django.conf.settings.DEBUG:
                user = None
            else:
                raise Exception('No valid user found')

        if op == 'getcapabilities':
            response = manager.get_capabilities()
        elif op == 'describeprocess':
            response = manager.describe_process(identifier)
        else:
            response = manager.execute(user, identifier, data_inputs)
    except node_manager.NodeManagerWPSError as e:
        logger.exception('Specific WPS error')
        # Custom WPS error
        response = e.exc_report.xml()
    except django.db.ProgrammingError:
        exc_report = metadata.ExceptionReport(settings.VERSION)

        exc_report.add_exception(metadata.NoApplicableCode, 'Database has not been initialized')

        response = exc_report.xml()
    except Exception as e:
        logger.exception('General WPS error')

        exc_report = metadata.ExceptionReport(settings.VERSION)
        
        exc_report.add_exception(metadata.NoApplicableCode, e.message)

        response = exc_report.xml()

    return http.HttpResponse(response, content_type='text/xml')

@require_http_methods(['GET'])
def regen_capabilities(request):
    servers = models.Server.objects.all()

    for s in servers:
        tasks.capabilities.delay(s.id)

    return http.HttpResponse('Regenerated capabilities')

@require_http_methods(['GET'])
def status(request, job_id):
    manager = node_manager.NodeManager()

    status = manager.get_status(job_id)

    return http.HttpResponse(status, content_type='text/xml')

@require_http_methods(['GET'])
def jobs(request):
    jobs = get_list_or_404(models.Job)

    data = []

    for j in jobs:
        history = [dict(pk=x.pk, status=x.status, created_date=x.created_date, result=x.result)
                for x in j.status_set.all()]

        data.append(dict(pk=j.pk, server=j.server.host, history=history))

    return http.JsonResponse(data, safe=False)

@require_http_methods(['GET'])
def processes(request):
    processes = get_list_or_404(models.Process)

    data = serializers.serialize('json', processes)

    return http.HttpResponse(data, content_type='application/json')

@require_http_methods(['GET'])
def instances(request):
    instances = get_list_or_404(models.Instance)

    data = serializers.serialize('json', instances)

    return http.HttpResponse(data, content_type='application/json')

@require_http_methods(['GET'])
def servers(request):
    servers = get_list_or_404(models.Server)

    data = serializers.serialize('json', servers)

    return http.HttpResponse(data, content_type='application/json')

def output(request, file_name):
    return serve(request, file_name, document_root=settings.OUTPUT_LOCAL_PATH)

def debug(request):
    return render(request, 'wps/debug.html')
