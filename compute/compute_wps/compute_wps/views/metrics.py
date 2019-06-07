#! /usr/bin/env python

import prometheus_client
from django import http
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from compute_wps import metrics


@require_http_methods(['GET'])
@ensure_csrf_cookie
def metrics_view(request):
    response = prometheus_client.generate_latest(metrics.WPS)

    return http.HttpResponse(response, content_type=prometheus_client.CONTENT_TYPE_LATEST)
