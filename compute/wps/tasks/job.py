#! /usr/bin/env python

import json
import os
import shutil

import cwt
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.mail import EmailMessage

from wps import metrics
from wps import models
from wps import WPSError
from wps.tasks import base

logger = get_task_logger('wps.tasks.job')

JOB_SUCCESS_MSG = """
Hello {name},

<pre>
Job {job.pk} has finished successfully.
</pre>

<pre>
{outputs}
</pre>

<pre>
<a href="{settings.WPS_JOB_URL}">View job history</a>
</pre>

<pre>
Thank you,
ESGF Compute Team
</pre>
"""

JOB_FAILED_MSG = """
Hello {name},

<pre>
Job {job.pk} has failed.
</pre>

<pre>
{error}
</pre>

Report errors on <a href="https://github.com/ESGF/esgf-compute-wps/issues/new?template=Bug_report.md">GitHub</a>.

<pre>
Thank you,
ESGF Compute Team
</pre>
"""

def send_failed_email(context, error):
    if context.user.first_name is None:
        name = context.user.username
    else:
        name = context.user.get_full_name()

    msg = JOB_FAILED_MSG.format(name=name, job=context.job, error=error)

    email = EmailMessage('Job Failed', msg, to=[context.user.email,])

    email.content_subtype = 'html'

    email.send(fail_silently=True)

def send_success_email(context, variable):
    if context.user.first_name is None:
        name = context.user.username
    else:
        name = context.user.get_full_name()

    outputs = '\n'.join('<a href="{!s}.html">{!s}|{!s}</a>'.format(
        x.uri,
        x.var_name,
        x.name) 
        for x in variable)

    msg = JOB_SUCCESS_MSG.format(name=name, job=context.job, settings=settings,
                                outputs=outputs)

    email = EmailMessage('Job Success', msg, to=[context.user.email,])

    email.content_subtype = 'html'

    email.send(fail_silently=True)

@base.cwt_shared_task()
def job_started(self, context):
    context.job.started()

    return context

@base.cwt_shared_task()
def job_succeeded_workflow(self, context):
    context.job.succeeded(json.dumps({
        'outputs': [x.parameterize() for x in
                    context.output+context.intermediate.values()],
    }))

    send_success_email(context, context.output)

    context.process.track(context.user)

    for variable in context.variable.values():
        models.File.track(context.user, variable)

        metrics.track_file(variable)

    return context

@base.cwt_shared_task()
def job_succeeded(self, context):
    relpath = os.path.relpath(context.output_path, settings.WPS_PUBLIC_PATH)

    url = settings.WPS_DAP_URL.format(filename=relpath)

    output = cwt.Variable(url, context.inputs[0].variable.var_name)

    context.job.succeeded(json.dumps(output.parameterize()))

    send_success_email(context, [output,])

    context.process.track(context.user)

    if context.operation.get_parameter('intermediate') is None:
        for input in context.inputs:
            models.File.track(context.user, input.variable)

            metrics.track_file(input.variable)

    return context
