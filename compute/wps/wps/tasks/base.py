#! /usr/bin/env python

from builtins import str
from contextlib import contextmanager
from functools import partial

import cdms2
import celery
from celery import shared_task
from celery.utils.log import get_task_logger

from wps import context
from wps import WPSError
from wps.util import wps_response

logger = get_task_logger('wps.tasks.base')

REGISTRY = {}


def get_process(identifier):
    try:
        return REGISTRY[identifier]
    except KeyError as e:
        raise WPSError('Unknown process {!r}', e)


def register_process(backend, process, **kwargs):
    def wrapper(func):
        func.BACKEND = backend

        func.PROCESS = process

        func.IDENTIFIER = '{!s}.{!s}'.format(backend, process)

        REGISTRY[func.IDENTIFIER] = func

        func.ABSTRACT = kwargs.get('abstract', '')

        func.METADATA = kwargs.get('metadata', {})

        return func

    return wrapper


class CWTBaseTask(celery.Task):

    @contextmanager
    def open(self, uri, mode='r', timeout=30):
        fd = cdms2.open(str(uri), mode)

        try:
            yield fd
        finally:
            fd.close()

    def status(self, fmt, *args, **kwargs):
        if isinstance(self.request.args[0], (list, tuple)):
            context = self.request.args[0][0]
        else:
            context = self.request.args[0]

        message = fmt.format(*args, **kwargs)

        logger.info('%s %r', message, context.job.progress)

        task_message = '[{}] {}'.format(self.request.id, message)

        context.job.update(task_message)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        logger.info('Retry %r', args)

        if len(args) > 0 and isinstance(args[0], context.OperationContext):
            args[0].job.retry(exc)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.info('Failure %r', args)

        if len(args) > 0 and isinstance(args[0], (context.OperationContext, context.WorkflowOperationContext)):
            args[0].job.failed(wps_response.exception_report(wps_response.NoApplicableCode, str(exc)))

            from wps.tasks import job

            job.send_failed_email(args[0], str(exc))

    def on_success(self, retval, task_id, args, kwargs):
        logger.info('Success %r', args)

        if len(args) > 0 and isinstance(args[0], context.OperationContext):
            args[0].job.step_complete()


cwt_shared_task = partial(shared_task, bind=True, base=CWTBaseTask)
