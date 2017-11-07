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

        if update:
            updated = request.session.get('updated', None)

            if updated is None:
                updated = datetime.datetime.now()
            else:
                updated = datetime.datetime.strptime(updated, SESSION_TIME_FMT)

            status = []

            for x in models.Job.objects.get(pk=job_id).status_set.filter(updated_date__gt=updated):
                data = {
                    'exception': x.exception,
                    'output': x.output,
                    'status': x.status,
                    'messages': []
                }
                
                for y in x.message_set.filter(created_date__gt=updated):
                    msg_data = {
                        'created_date': y.created_date,
                        'percent': y.percent,
                        'message': y.message
                    }

                    data['messages'].append(msg_data)

                status.append(data)

            if len(status) > 0 and len(status[-1]['messages']) > 0:
                request.session['updated'] = status[-1]['messages'][-1]['created_date'].strftime(SESSION_TIME_FMT)
            else:
                request.session['updated'] = datetime.datetime.now().strftime(SESSION_TIME_FMT)
        else:
            status = []

            status_qs = models.Job.objects.get(pk=job_id).status_set.all()

            for x in models.Job.objects.get(pk=job_id).status_set.all().order_by('created_date'):
                data = {
                    'created_date': x.created_date,
                    'status': x.status,
                    'exception': x.exception,
                    'output': x.output,
                    'messages': []
                }

                for y in x.message_set.all().order_by('created_date'):
                    msg_data = {
                        'created_date': y.created_date,
                        'percent': y.percent,
                        'message': y.message
                    }

                    data['messages'].append(msg_data)

                status.append(data)

            if len(status) > 0 and len(status[-1]['messages']) > 0:
                request.session['updated'] = status[-1]['messages'][-1]['created_date'].strftime(SESSION_TIME_FMT)
            else:
                request.session['updated'] = datetime.datetime.now().strftime(SESSION_TIME_FMT)
    except Exception as e:
        logger.exception('Error retrieving job details')

        return common.failed(e.message)
    else:
        return common.success(status)
