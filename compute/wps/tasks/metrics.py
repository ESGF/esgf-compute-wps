import json
import os
from datetime import datetime

import cdms2
import cwt
import requests
from cdms2 import MV2 as MV
from celery.task.control import inspect
from celery.utils.log import get_task_logger
from django.conf import settings
from django.utils import timezone

from wps import helpers
from wps import metrics
from wps import models
from wps import WPSError
from wps.tasks import base

logger = get_task_logger('wps.tasks.metrics')

def query_prometheus(**kwargs):
    response = requests.get(settings.METRICS_HOST, params=kwargs)

    if not response.ok:
        raise WPSError('Failed querying "{}" {}: {}', settings.METRICS_HOST,
                       response.reason, response.status_code)

    data = response.json()

    try:
        status = data['status']
    except KeyError:
        raise WPSError('Excepted JSON from prometheus request')

    logger.info('%r', data)

    value = data['data']['result'][0]['value'][1]

    return value

@base.register_process('CDAT.metrics', abstract="""
                       Returns the current metrics of the server.
                       """, data_inputs=[], metadata={'inputs': 0})
@base.cwt_shared_task()
def health(self, user_id, job_id, **kwargs):
    job = self.load_job(job_id)

    jobs_running = metrics.jobs_running()

    jobs_queued = metrics.jobs_queued()

    download = query_prometheus(query='sum(delta(wps_ingress_bytes[1d]))')

    upload = query_prometheus(query='sum(delta(wps_upload_bytes[1d]))')

    data = {
        'usage': {
            'files': None,
            'services': [],
            'data': {
                'units': 'Bytes',
                'download': download,
                'upload': upload,
            }
        },
        'health': {
            'users': None,
            'queued': jobs_queued,
            'running': jobs_running,
            'nodes': None,
            'cpu': None,
        },
        'time': datetime.now().ctime(),
    }

    job.succeeded(json.dumps(data))

    return data
