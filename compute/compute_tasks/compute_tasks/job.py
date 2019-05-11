#! /usr/bin/env python

import json

import cwt
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.mail import EmailMessage

from compute_tasks import base
from compute_tasks import WPSError
from compute_tasks.context import OperationContext
from compute_tasks.context import WorkflowOperationContext

logger = get_task_logger('compute_tasks.job')

JOB_SUCCESS_MSG = """
Hello {name},

<pre>
Job {job} has finished successfully.
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
Job {job} has failed.
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
    user = context.user_details()

    msg = JOB_FAILED_MSG.format(name=user['first_name'], job=context.job, error=error)

    email = EmailMessage('Job Failed', msg, to=[user['email'], ])

    email.content_subtype = 'html'

    email.send(fail_silently=True)


def send_success_email(context, variable):
    logger.info('Sending success email using variable %r', variable)

    user = context.user_details()

    if not isinstance(variable, (list, tuple)):
        variable = [variable, ]

    outputs = '\n'.join('<a href="{!s}.html">{!s}|{!s}</a>'.format(
        x.uri,
        x.var_name,
        x.name)
        for x in variable)

    msg = JOB_SUCCESS_MSG.format(name=user['first_name'], job=context.job, settings=settings, outputs=outputs)

    email = EmailMessage('Job Success', msg, to=[user['email'], ])

    email.content_subtype = 'html'

    email.send(fail_silently=True)


def send_success_email_data(context, outputs):
    user = context.user_details()

    msg = JOB_SUCCESS_MSG.format(name=user['first_name'], job=context.job, settings=settings, outputs=outputs)

    email = EmailMessage('Job Success', msg, to=[user['email'], ])

    email.content_subtype = 'html'

    email.send(fail_silently=True)


def build_context(identifier, data_inputs):
    variable = None
    domain = None
    operation = None

    for id in ('variable', 'domain', 'operation'):
        try:
            data = json.loads(data_inputs[id])
        except ValueError:
            raise WPSError('DataInput {!r} invalid format', id)

        if id == 'variable':
            data = [cwt.Variable.from_dict(x) for x in data]

            variable = dict((x.name, x) for x in data)
        elif id == 'domain':
            data = [cwt.Domain.from_dict(x) for x in data]

            domain = dict((x.name, x) for x in data)
        elif id == 'operation':
            data = [cwt.Process.from_dict(x) for x in data]

            operation = dict((x.name, x) for x in data)

    if identifier == 'CDAT.workflow' or len(operation) > 1:
        try:
            workflow_op = [x for x in operation.values() if x.identifier == 'CDAT.workflow'][0]
        except IndexError:
            raise WPSError('Some odd occurred expected CDAT.workflow operation but did not find it')

        # Remove the workflow operation as it's just a placeholder for global values
        operation.pop(workflow_op.name)

        context = WorkflowOperationContext.from_data_inputs(variable, domain, operation)
    else:
        context = OperationContext.from_data_inputs(identifier, variable, domain, operation)

    return context


@base.cwt_shared_task()
def job_started(self, identifier, data_inputs, job_id, user_id, process_id):
    context = build_context(identifier, data_inputs)

    data = {
        'job': job_id,
        'user': user_id,
        'process': process_id,
    }

    context.init_state(data)

    context.started()

    return context


@base.cwt_shared_task()
def job_succeeded(self, context):
    if len(context.output) == 1:
        if isinstance(context.output[0], cwt.Variable):
            context.succeeded(json.dumps(context.output[0].to_dict()))

            send_success_email(context, context.output)
        elif isinstance(context.output[0], str):
            context.succeeded(context.output)

            send_success_email_data(context, context.output)
    else:
        context.job.succeeded(json.dumps([x.to_dict() for x in context.output]))

        send_success_email(context, context.output)

    from compute_tasks import context as ctx

    context.update_metrics(ctx.SUCCESS)

    return context
