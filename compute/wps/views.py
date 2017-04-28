import json
import logging
import re

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

@require_http_methods(['GET'])
def oauth2_callback(request):
    try:
        oid = request.session.pop('openid')

        oid_response = request.session.pop('openid_response')

        oauth_state = request.session.pop('oauth_state')
    except KeyError as e:
        logger.debug('Session did not contain key "%s"', e.message)

        return redirect('login')

    manager = node_manager.NodeManager()

    api_key = manager.auth_oauth2_callback(oid, oid_response, request.META['QUERY_STRING'], oauth_state)

    return render(request, 'wps/login_result.html', { 'api_key': api_key })

@require_http_methods(['GET'])
def login(request):
    return render(request, 'wps/login.html')

@require_http_methods(['GET', 'POST'])
def login_oauth2(request):
    if request.method == 'POST':
        form = forms.OpenIDForm(request.POST)

        if form.is_valid():
            oid_url = form.cleaned_data.get('openid')

            manager = node_manager.NodeManager()

            redirect_url, session = manager.auth_oauth2(oid_url)

            request.session.update(session)

            return redirect(redirect_url)
    else:
        form = forms.OpenIDForm()

    return render(request, 'wps/login_form.html', { 'form': form, 'action': 'oauth2' })

@require_http_methods(['GET', 'POST'])
def login_mpc(request):
    if request.method == 'POST':
        form = forms.MPCForm(request.POST)

        if form.is_valid():
            oid_url = form.cleaned_data.get('openid')

            username = form.cleaned_data.get('username')

            password = form.cleaned_data.get('password')

            manager = node_manager.NodeManager()

            api_key = manager.auth_mpc(oid_url, username, password)

            return render(request, 'wps/login_result.html', { 'api_key': api_key })
    else:
        form = forms.MPCForm()

    return render(request, 'wps/login_form.html', { 'form': form, 'action': 'mpc' })

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps(request):
    manager = node_manager.NodeManager()

    try:
        api_key, op, identifier, data_inputs = manager.handle_request(request)

        logger.info('Transformed WPS request Operation: %s Identifier: %s '
                'DataInputs: %s', op, identifier, data_inputs)

        try:
            user = models.User.objects.filter(auth__api_key=api_key)[0]
        except IndexError:
            raise Exception('Unabled to find a user with the api key {}'.format(api_key))

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
