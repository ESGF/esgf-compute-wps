#! /usr/bin/env python

import json
import os
import shutil

import cwt
from celery.utils.log import get_task_logger
from django.conf import settings

from wps import metrics
from wps.tasks import base

logger = get_task_logger('wps.tasks.job')

@base.cwt_shared_task()
def job_error(**kwargs):
    logger.info('Error %r', kwargs)

@base.cwt_shared_task()
def job_started(self, job_id):
    job = self.load_job(job_id)

    job.started()

    metrics.JOBS_QUEUED.set(metrics.jobs_queued())

    metrics.JOBS_RUNNING.set(metrics.jobs_running())

@base.cwt_shared_task()
def job_succeeded(self, attrs, output_path, move_path, var_name, job_id):
    job = self.load_job(job_id)

    if move_path is not None:
        shutil.move(output_path, move_path)
        
        _, filename = os.path.split(move_path)

        url = settings.WPS_DAP_URL.format(filename=filename)
    else:
        _, filename = os.path.split(output_path)

        url = settings.WPS_DAP_URL.format(filename=filename)

    output = cwt.Variable(url, var_name)

    job.succeeded(json.dumps(output.parameterize()))

    metrics.JOBS_RUNNING.set(metrics.jobs_running())

    return attrs
