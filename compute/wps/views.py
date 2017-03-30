import json
import logging

from django import http
from django.core import serializers
from django.shortcuts import render
from django.shortcuts import redirect
from django.shortcuts import get_list_or_404
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from cwt.wps_lib import metadata

from wps import forms
from wps import models
from wps import node_manager
from wps import settings
from wps import wps_xml
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

        oauth_state = request.session.pop('oauth_state')
    except KeyError as e:
        logger.debug('Session did not contain key "%s"', e.message)

        return redirect('login')

    request_url = '{0}?{1}'.format(settings.OAUTH2_CALLBACK,
            request.META['QUERY_STRING'])

    try:
        oid = openid.OpenID.parse(openid_url)

        token_service = oid.find(URN_ACCESS)
    except openid.OpenIDError:
        return http.HttpResponseBadRequest('Unable to retrieve token url from OpenID metadata')

    try:
        token = oauth2.token_from_openid(token_service.uri, request_url, oauth_state) 
    except oauth2.OAuth2Error:
        return http.HttpResponseBadRequest('OAuth2 callback was not passed the correct parameters')

    manager = node_manager.NodeManager()

    api_key = manager.create_user(openid_url, token)

    return http.HttpResponse('Your new api key: {}'.format(api_key))

@require_http_methods(['GET', 'POST'])
def oauth2_login(request):
    if request.method == 'POST':
        form = forms.OpenIDForm(request.POST)

        if form.is_valid():
            openid_url = form.cleaned_data['openid']

            try:
                oid = openid.OpenID.parse(openid_url)

                auth_service = oid.find(URN_AUTHORIZE)

                cert_service = oid.find(URN_RESOURCE)
            except openid.OpenIDError:
                return http.HttpResponseBadRequest('Unable to retrieve authorization and certificate urls from OpenID metadata')

            try:
                auth_url, state = oauth2.get_authorization_url(auth_service.uri, cert_service.uri)
            except oauth2.OAuth2Error:
                return http.HttpResponseBadRequest('Could not retrieve the OAuth2 authorization url')

            if 'oauth_state' in request.session:
                del request.session['oauth_state']

            request.session['oauth_state'] = state

            if 'openid' in request.session:
                del request.session['openid']

            request.session['openid'] = openid_url

            return redirect(auth_url)
    else:
        form = forms.OpenIDForm()

    return render(request, 'wps/login.html', { 'form': form })

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps(request):
    manager = node_manager.NodeManager()

    try:
        response = manager.handle_request(request)
    except node_manager.NodeManagerError as e:
        # NodeManagerError should always contain ExceptionReport xml
        response = e.message
    except Exception as e:
        # Handle any generic exceptions, a catch-all
        report = metadata.ExceptionReport(wps_xml.VERSION)

        report.add_exception(metadata.NoApplicableCode, e.message)

        response = report.xml()

    return http.HttpResponse(response, content_type='text/xml')

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

def debug(request):
    return render(request, 'wps/debug.html')
