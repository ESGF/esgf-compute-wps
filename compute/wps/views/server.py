import logging

from django import http
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from wps import models
from wps import WPSError
from wps.views import common

logger = logging.getLogger('wps.views')

@ensure_csrf_cookie
def home(request):
    return render(request, 'index.html')
