#! /usr/bin/env python

import json
import re
import signal
from contextlib import contextmanager
from datetime import datetime
from functools import partial

import cdms2
import celery
import cwt
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import URLValidator

from wps import context
from wps import metrics
from wps import models
from wps import AccessError
from wps import WPSError
from wps.util import wps_response

logger = get_task_logger('wps.tasks.base')

REGISTRY = {}

def get_process(identifier):
    try:
        return REGISTRY[identifier]
    except KeyError as e:
        raise WPSError('Missing process "{identifier}"', identifier=identifier)

def register_process(identifier, **kwargs):
    def wrapper(func):
        REGISTRY[identifier] = func

        func.IDENTIFIER = identifier

        func.ABSTRACT = kwargs.get('abstract', '')

        func.PROCESS = kwargs.get('process')

        func.METADATA = kwargs.get('metadata', {})

        return func

    return wrapper

class Timeout(object):
    def __init__(self, seconds, url):
        self.seconds = seconds
        self.url = url

    def handle_timeout(self, signum, frame):
        raise AccessError(url, '')

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
    
    def __exit__(self, exc_type, exc_value, traceback):
        signal.alarm(0)

class CWTBaseTask(celery.Task):

    @contextmanager
    def open(self, uri, mode='r', timeout=30):
        #with Timeout(timeout, uri):
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
