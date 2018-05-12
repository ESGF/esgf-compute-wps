#! /usr/bin/env python

import cdms2
import cwt
import requests
from django.conf import settings

from wps.tasks import base

__ALL__ = [
    'preprocess',
    'ingress',
]

@base.cwt_shared_task()
def preprocess(self, parent_variables, variables, domains, operation, user_id, job_id):
    self.PUBLISH = base.ALL

    _, _, o = self.load(parent_variables, variables, domains, operation)

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id)

    chunk_map = proc.generate_chunk_map(o)

    data = {
        'chunk_map': chunk_map,
        'operation': operation,
        'user_id': user_id,
        'job_id': job_id
    }

    response = requests.post(settings.WPS_INGRESS_URL, data)

    if not response.ok:
        raise base.WPSError('Failed to ingress data')

@base.cwt_shared_task()
def ingress(self, input_url, var_name, domain, output_uri):
    self.PUBLISH = base.ALL

    temporal = domain['temporal']

    spatial = domain['spatial']

    with cdms2.open(input_url) as infile, cdms2.open(output_uri, 'w') as outfile:
        data = infile(var_name, time=temporal, **spatial)

        outfile.write(var_name, data)

    variable = cwt.Variable(output_uri, var_name)

    return variable.parameterize()
