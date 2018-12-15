#! /usr/bin/env python

import json
import os
import shutil

import cwt
from celery.utils.log import get_task_logger
from django.conf import settings

from wps import metrics
from wps import models
from wps import WPSError
from wps.tasks import base

logger = get_task_logger('wps.tasks.job')

@base.cwt_shared_task()
def job_started(self, context):
    context.job.started()

    return context

@base.cwt_shared_task()
def job_succeeded_workflow(self, attrs, variables, process_id, user_id, job_id):
    job = self.load_job(job_id)

    user = self.load_user(user_id)

    process = self.load_process(process_id)

    job.succeeded(json.dumps({
        'outputs': [x.parameterize() for x in attrs['output']],
    }))

    metrics.JOBS_RUNNING.set(metrics.jobs_running())

    process.track(user)

    for var in variables:
        models.File.track(user, var)

    return attrs

@base.cwt_shared_task()
def job_succeeded(self, context):
    relpath = os.path.relpath(context.output_path, settings.WPS_PUBLIC_PATH)

    url = settings.WPS_DAP_URL.format(filename=relpath)

    output = cwt.Variable(url, context.inputs[0].variable.var_name)

    context.job.succeeded(json.dumps(output.parameterize()))

    context.process.track(context.user)

    for input in context.inputs:
        models.File.track(context.user, input.variable)

        metrics.track_file(input.variable)

    return context
