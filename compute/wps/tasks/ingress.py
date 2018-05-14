#! /usr/bin/env python

import cdms2
import cwt
import json
import requests
from django.conf import settings
from celery.utils import log

from wps import helpers
from wps.tasks import base
from wps.tasks import process

__ALL__ = [
    'preprocess',
    'ingress',
]

logger = log.get_task_logger('wps.tasks.ingress')

@base.cwt_shared_task()
def preprocess(self, parent_variables, variables, domains, operation, user_id, job_id):
    self.PUBLISH = base.FAILURE | base.RETRY

    _, _, o = self.load(parent_variables, variables, domains, operation)

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id)

    chunk_map = proc.generate_chunk_map(o)

    o.domain = None

    if 'domain' in o.parameters:
        del o.parameters['domain']

    o.inputs = []

    data = {
        'chunk_map': json.dumps(chunk_map, default=helpers.json_dumps_default),
        'operation': json.dumps(o.parameterize()),
        'user_id': user_id,
        'job_id': job_id
    }

    session = requests.Session()

    response = session.get(settings.WPS_ENDPOINT, verify=False)

    csrf_token = session.cookies.get('csrftoken')

    headers = { 'X-CSRFToken': csrf_token }

    response = session.post(settings.WPS_INGRESS_URL, data, headers=headers, verify=False)

    if not response.ok:
        raise base.WPSError('Failed to ingress data status code {code}', code=response.status_code)

@base.cwt_shared_task()
def ingress(self, input_url, var_name, domain, output_uri):
    self.PUBLISH = base.FAILURE | base.RETRY

    temporal = domain['temporal']

    spatial = domain['spatial']

    with cdms2.open(input_url) as infile, cdms2.open(output_uri, 'w') as outfile:
        data = infile(var_name, time=temporal, **spatial)

        outfile.write(var_name, data)

    variable = cwt.Variable(output_uri, var_name)

    return variable.parameterize()
