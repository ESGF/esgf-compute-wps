#! /usr/bin/env python

import importlib
import json
import os
import pkgutil
from builtins import str
from functools import partial

import celery
from celery import shared_task
from celery.utils.log import get_task_logger

from wps import context
from wps import WPSError
from wps.util import wps_response

logger = get_task_logger('wps.tasks.base')

REGISTRY = {}
BINDINGS = {}


def discover_processes():
    processes = []

    for _, name, _ in pkgutil.iter_modules([os.path.dirname(__file__)]):
        if name == 'base':
            continue

        mod = importlib.import_module('.{!s}'.format(name), package='wps.tasks')

        if 'discover_processes' in dir(mod):
            method = getattr(mod, 'discover_processes')

            data = method()

            processes.extend(data)

    processes.extend(REGISTRY.values())

    return processes


def build_process_bindings():
    for _, name, _ in pkgutil.iter_modules([os.path.dirname(__file__)]):
        if name == 'base':
            continue

        mod = importlib.import_module('.{!s}'.format(name), package='wps.tasks')

        if 'process_bindings' in dir(mod):
            method = getattr(mod, 'process_bindings')

            try:
                data = method()
            except Exception:
                continue

            BINDINGS.update(data)


def get_process(identifier):
    try:
        return BINDINGS[identifier]
    except KeyError as e:
        raise WPSError('Unknown process {!r}', e)


def register_process(backend, process, **kwargs):
    def wrapper(func):
        identifier = '{!s}.{!s}'.format(backend, process)

        REGISTRY[identifier] = {
            'identifier': identifier,
            'backend': backend,
            'abstract': kwargs.get('abstract', ''),
            'metadata': json.dumps(kwargs.get('metadata', {})),
        }

        return func

    return wrapper


class CWTBaseTask(celery.Task):
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
