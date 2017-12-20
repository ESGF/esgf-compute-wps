import logging

from django import http
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from wps import models
from wps import WPSError
from wps.views import common

logger = logging.getLogger('wps.views')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def processes(request):
    try:
        common.authentication_required(request)

        data = [dict(identifier=x.identifier, description=x.description) for x in models.Process.objects.all() if x.enabled]
    except WPSError as e:
        logger.exception('Error retrieving processes')

        return common.failed(e.message)
    else:
        return common.success(data)

@ensure_csrf_cookie
def output(request, file_name):
    return serve(request, file_name, document_root=settings.OUTPUT_LOCAL_PATH)

@ensure_csrf_cookie
def home(request):
    return render(request, 'index.html')
