#! /usr/bin/env python

import cdms2
import cwt
import json
import requests
from django.conf import settings
from celery.utils import log

from wps import helpers
from wps import models
from wps.tasks import base
from wps.tasks import process

__ALL__ = [
    'preprocess',
    'ingress',
]

logger = log.get_task_logger('wps.tasks.ingress')

def is_workflow(operations):
    root_node = None

    operations = dict((x, cwt.Process.from_dict(y)) for x, y in operations.iteritems())

    # flatten out list of inputs from operations
    op_inputs = [i for op in operations.values() for i in op.inputs]

    # find the root operation, this node will not be an input to any other operation
    for op in operations.values():
        if op.name not in op_inputs:
            if root_node is not None:
                raise base.WPSError('Dangling operations, there can only be a single operation that is not an input')

            root_node = op

    if root_node is None:
        raise base.WPSError('Malformed WPS execute request, atleast one operation must be provided')

    # considered a workflow if any of the root operations inputs are another operation
    return root_node, any(i in operations.keys() for i in root_node.inputs)

@base.cwt_shared_task()
def preprocess(self, identifier, variables, domains, operations, user_id, job_id):
    workflow = False
    ingress = False

    self.PUBLISH = base.RETRY | base.FAILURE

    _, _, o = self.load({}, variables, domains, operations)

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id)

    try:
        proc_obj = models.Process.objects.get(identifier=identifier)
    except models.Process.DoesNotExist:
        raise WPSError('Process "{identifier}" does not exist', identifier=identifier)

    proc_obj.track(proc.user)
    
    for variable in o.inputs:
        models.File.track(proc.user, variable)

    root_node, workflow = is_workflow(operations)

    if workflow:
        logger.info('Detected workflow whose output process is "%r"', root_node.identifier)

        data = {
            'root_node': json.dumps(root_node),
            'variables': json.dumps(variables),
            'domains': json.dumps(domains),
            'operations': json.dumps(operations),
            'user_id': user_id,
            'job_id': job_id,
        }

        url = settings.WPS_WORKFLOW_URL
    else:
        if ingress:
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

            url = settings.WPS_INGRESS_URL
        else:
            try:
                operation = operations.values()[0]
            except IndexError:
                raise base.WPSError('Missing operation "{identifier}"', identifier=identifier)

            data = {
                'variables': json.dumps(variables),
                'domains': json.dumps(domains),
                'operation': json.dumps(operation),
                'user_id': user_id,
                'job_id': job_id,
            }

            url = settings.WPS_EXECUTE_URL

    session = requests.Session()

    response = session.get(settings.WPS_ENDPOINT, verify=False)

    csrf_token = session.cookies.get('csrftoken')

    headers = { 'X-CSRFToken': csrf_token }

    response = session.post(url, data, headers=headers, verify=False)

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
