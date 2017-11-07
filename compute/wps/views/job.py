#! /usr/bin/env python

import datetime

from django import http
from django.db.models import Max
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common

from wps import models

logger = common.logger

SESSION_TIME_FMT = '%Y%m%d%H%M%S'

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

        update = request.GET.get('update', False)

        job = models.Job.objects.get(pk=job_id)

        if update:
            pass
        else:
            status = job.status

    except Exception as e:
        logger.exception('Error retrieving job details')

        return common.failed(e.message)
    else:
        return common.success(status)
