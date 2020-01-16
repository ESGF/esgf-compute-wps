#! /usr/bin/env python

import json

import cwt
from celery.utils.log import get_task_logger

from compute_tasks import base

logger = get_task_logger('compute_tasks.job')


@base.cwt_shared_task()
def job_started(self, context):
    context.started()

    return context


@base.cwt_shared_task()
def job_succeeded(self, context):
    if len(context.output) == 1:
        if isinstance(context.output[0], cwt.Variable):
            context.succeeded(json.dumps(context.output[0].to_dict()))
        elif isinstance(context.output[0], str):
            context.succeeded(context.output[0])
    else:
        context.succeeded(json.dumps([x.to_dict() for x in context.output]))

    from compute_tasks import context as ctx

    context.update_metrics(ctx.SUCCESS)

    return context
