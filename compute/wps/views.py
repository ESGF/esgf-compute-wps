import json
import logging

from django import http
from django.core import serializers
from django.shortcuts import render
from django.shortcuts import get_list_or_404
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from cwt.wps_lib import metadata

from wps import models
from wps import node_manager
from wps import wps_xml

logger = logging.getLogger(__name__)

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

def status(request, job_id):
    manager = node_manager.NodeManager()

    status = manager.get_status(job_id)

    return http.HttpResponse(status, content_type='text/xml')

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
