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

@require_http_methods(['GET'])
@ensure_csrf_cookie
def jobs(request):
    try:
        common.authentication_required(request)

        jobs_qs = models.Job.objects.filter(user_id=request.user.id)

        jobs_qs = jobs_qs.annotate(created_date=Max('status__created_date')).order_by('-created_date')

        jobs = []

        for x in jobs_qs:
            data = x.details

            data.update({'created_date': x.created_date})

            jobs.append(data)
    except Exception as e:
        logger.exception('Error retrieving jobs')

        return common.failed(e.message)
    else:
        return common.success(jobs)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def job(request, job_id):
    try:
        common.authentication_required(request)

        update = request.GET.get('update', 'false')

        if update.lower() == 'false':
            update = False
        else:
            update = True

        job = models.Job.objects.get(pk=job_id)

        if update:
            updated = request.session.get('updated', None)

            if updated is None:
                status = job.status
            else:
                status = job.statusSince(updated)

            if len(status) > 0:
                request.session['updated'] = status[-1]['updated_date']
        else:
            status = job.status

            if len(status) > 0:
                request.session['updated'] = status[-1]['updated_date']
            else:
                request.session['updated'] = None

    except Exception as e:
        logger.exception('Error retrieving job details')

        return common.failed(e.message)
    else:
        return common.success(status)
