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
from django.conf import settings

from wps import AccessError
from wps import WPSError

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
            setting_name = 'WPS_{!s}_ENABLED'.format(name.upper())

            enabled = getattr(settings, setting_name, False)

            if not enabled:
                logger.info('Skipping process discovery for module %r', name)

                continue

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
            setting_name = 'WPS_{!s}_ENABLED'.format(name.upper())

            enabled = getattr(settings, setting_name, False)

            if not enabled:
                logger.info('Skipping process binding for module %r', name)

                continue

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


def register_process(backend, process, abstract=None, version=None, inputs=None, **metadata):
    def wrapper(func):
        identifier = '{!s}.{!s}'.format(backend, process)

        metadata['inputs'] = inputs or 0

        REGISTRY[identifier] = {
            'identifier': identifier,
            'backend': backend,
            'abstract': abstract or '',
            'metadata': json.dumps(metadata),
            'version': version or 'devel',
        }

        BINDINGS[identifier] = func

        return func

    return wrapper


class CWTBaseTask(celery.Task):
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        logger.info('Retry %r', args)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.info('Failure %r %r', args, exc)

        try:
            args[0].failed(str(exc))
        except AttributeError:
            logger.exception('First argument should be OperationContext or WorkflowOperationContext')
        except WPSError:
            raise
        else:
            from wps.tasks import job

            job.send_failed_email(args[0], str(exc))

    def on_success(self, retval, task_id, args, kwargs):
        pass


cwt_shared_task = partial(shared_task,
                          bind=True,
                          base=CWTBaseTask,
                          autoretry_for=(AccessError, ),
                          retry_kwargs={'max_retries': 4},
                          retry_backoff=True)
