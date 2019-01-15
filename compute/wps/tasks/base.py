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
from wps.util import wps as wps_util

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

        func.DATA_INPUTS = kwargs.get('data_inputs')

        func.PROCESS_OUTPUTS = kwargs.get('process_outputs')

        func.PROCESS = kwargs.get('process')

        func.METADATA = kwargs.get('metadata', {})

        func.HIDDEN = kwargs.get('hidden', False)

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

    def update(self, job, fmt, *args, **kwargs):
        message = fmt.format(*args)

        tagged_message = '[{}] {}'.format(self.request.id, message)

        job.update(tagged_message)

        logger.info('%s %r', message, job.steps_progress)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        if len(args) > 0 and isinstance(args[0], context.OperationContext):
            args[0].job.retry(exc)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        if len(args) > 0 and isinstance(args[0], context.OperationContext):
            args[0].job.failed(wps_util.exception_report(settings, str(exc), cwt.ows.NoApplicableCode))

            from wps.tasks import job

            job.send_failed_email(args[0], str(exc))

    def on_success(self, retval, task_id, args, kwargs):
        if len(args) > 0 and isinstance(args[0], context.OperationContext):
            args[0].job.steps_inc()

cwt_shared_task = partial(shared_task, bind=True, base=CWTBaseTask)
