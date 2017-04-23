#! /usr/bin/env python

import json

import celery
import cwt
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from cwt.wps_lib import metadata

from wps import models
from wps import wps_xml

__all__ = ['REGISTRY', 'register_process', 'get_process', 'CWTBaseTask', 'handle_output']

logger = get_task_logger(__name__)

REGISTRY = {}

def get_process(name):
    try:
        return REGISTRY[name]
    except KeyError:
        raise Exception('Process {} does not exist'.format(name))

def register_process(name):
    def wrapper(func):
        REGISTRY[name] = func

        return func

    return wrapper

if settings.DEBUG:
    @register_process('wps.demo')
    @shared_task
    def demo(variables, operations, domains):
        logger.info('Operations {}'.format(operations))

        logger.info('Domains {}'.format(domains))

        logger.info('Variables {}'.format(variables))

        return cwt.Variable('file:///demo.nc', 'tas').parameterize()

class CWTBaseTask(celery.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        try:
            job = models.Job.objects.get(pk=kwargs['job_id'])
        except KeyError:
            raise Exception('Job id was not passed to the task')
        except models.Job.DoesNotExist:
            raise Exception('Job {} does not exist'.format(kwargs['job_id']))

        job.status_failed(exc)

@shared_task
def handle_output(variable, job_id):
    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        raise Exception('Job does not exist {}'.format(job_id))

    latest = job.status_set.all().latest('created_date')
    
    updated = wps_xml.update_execute_response(latest.result, json.dumps(variable))

    job.status_set.create(status=str(updated.status), result=updated.xml())
