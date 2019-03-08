#! /usr/bin/env python

import datetime

from django import http
from django.db.models import Max
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods
from rest_framework import viewsets

from . import common
from wps import models
from wps import serializers

logger = common.logger

SESSION_TIME_FMT = '%Y%m%d%H%M%S'

class StatusViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = models.Status.objects.all()
    serializer_class = serializers.StatusSerializer

class JobViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = models.Job.objects.all()
    serializer_class = serializers.JobSerializer
