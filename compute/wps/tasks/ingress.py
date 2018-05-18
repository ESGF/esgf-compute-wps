#! /usr/bin/env python

import cdms2
import cwt
import hashlib
import json
import os
import requests
import uuid
from django.conf import settings
from celery.utils import log

from wps import helpers
from wps import models
from wps.tasks import base
from wps.tasks import process
from wps.tasks import file_manager

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
    self.PUBLISH = base.RETRY | base.FAILURE

    logger.info('Preprocessing job %s user %s', job_id, user_id)

    proc = process.Process(self.request.id)

    proc.initialize(user_id, job_id)

    root_node, workflow = is_workflow(operations)

    if workflow:
        logger.info('Setting up a workflow pipeline')

        data = {
            'type': 'workflow',
            'root_node': json.dumps(root_node.parameterize()),
            'variables': json.dumps(variables),
            'domains': json.dumps(domains),
            'operations': json.dumps(operations),
            'user_id': user_id,
            'job_id': job_id,
        }

        raise base.WPSError('Workflow disabled')
    else:
        logger.info('Setting up a single process pipeline')

        _, _, o = self.load({}, variables, domains, operations.values()[0])

        if not proc.check_cache(o):
            logger.info('Configuring an ingress pipeline')

            chunk_map = proc.generate_chunk_map(o)

            o.inputs = []

            data = {
                'type': 'ingress',
                'chunk_map': json.dumps(chunk_map, default=helpers.json_dumps_default),
                'domains': json.dumps(domains),
                'operation': json.dumps(o.parameterize()),
                'user_id': user_id,
                'job_id': job_id
            }
        else:
            logger.info('Configuring an execute pipeline')

            try:
                operation = operations.values()[0]
            except IndexError:
                raise base.WPSError('Missing operation "{identifier}"', identifier=identifier)

            data = {
                'type': 'execute',
                'variables': json.dumps(variables),
                'domains': json.dumps(domains),
                'operation': json.dumps(operation),
                'user_id': user_id,
                'job_id': job_id,
            }

    session = requests.Session()

    response = session.get(settings.WPS_ENDPOINT, verify=False)

    csrf_token = session.cookies.get('csrftoken')

    logger.info('Retrieved CSRF token')

    headers = { 'X-CSRFToken': csrf_token }

    response = session.post(settings.WPS_EXECUTE_URL, data, headers=headers, verify=False)

    if not response.ok:
        raise base.WPSError('Failed to ingress data status code {code}', code=response.status_code)

    logger.info('Successfuly submitted the execute request')

@base.cwt_shared_task()
def ingress(self, input_url, var_name, domain, base_units, output_uri):
    self.PUBLISH = base.RETRY | base.FAILURE

    logger.info('Ingress "%s" from %s', var_name, input_url)

    domain = json.loads(domain, object_hook=helpers.json_loads_object_hook)

    temporal = domain['temporal']

    spatial = domain['spatial']

    try:
        with cdms2.open(input_url) as infile, cdms2.open(output_uri, 'w') as outfile:
            data = infile(var_name, time=temporal, **spatial)

            data.getTime().toRelativeTime(base_units)

            outfile.write(data, id=var_name)
    except cdms2.CDMSError as e:
        raise base.AccessError('', e.message)

    variable = cwt.Variable(output_uri, var_name)

    return { variable.name: variable.parameterize() }

@base.cwt_shared_task()
def ingress_cache(self, ingress_chunks, ingress_map, job_id):
    self.PUBLISH = base.ALL

    logger.info('Generating cache files from ingressed data')

    collection = file_manager.DataSetCollection()

    variable_name = None
    variables = {}

    for chunk in ingress_chunks:
        variables.update(chunk)

    variables = [cwt.Variable.from_dict(y) for _, y in variables.iteritems()]

    ingress_map = json.loads(ingress_map, object_hook=helpers.json_loads_object_hook)

    output_name = '{}.nc'.format(str(uuid.uuid4()))

    output_path = os.path.join(settings.WPS_LOCAL_OUTPUT_PATH, output_name)

    output_url = settings.WPS_DAP_URL.format(filename=output_name)

    logger.info('Writing output to %s', output_path)

    try:
        with cdms2.open(output_path, 'w') as outfile:
            for url in sorted(ingress_map.keys()):
                meta = ingress_map[url]

                logger.info('Processing source input %s', url)

                cache = None
                cache_obj = None

                if variable_name is None:
                    variable_name = meta['variable_name']

                dataset = file_manager.DataSet(cwt.Variable(url, variable_name))

                dataset.temporal = meta['temporal']

                dataset.spatial = meta['spatial']

                logger.info('Processing "%s" ingressed chunks of data', len(meta['ingress_chunks']))

                # Try/except to handle closing of cache_obj
                try:
                    for chunk in meta['ingress_chunks']:
                        # Try/except to handle opening of chunk
                        try:
                            with cdms2.open(chunk) as infile:
                                if cache is None:
                                    dataset.file_obj = infile

                                    domain = collection.generate_dataset_domain(dataset)

                                    dimensions = json.dumps(domain, default=models.slice_default)

                                    uid = '{}:{}'.format(dataset.url, dataset.variable_name)

                                    uid_hash = hashlib.sha256(uid).hexdigest()

                                    cache = models.Cache.objects.create(uid=uid_hash, url=dataset.url, dimensions=dimensions)

                                    try:
                                        cache_obj = cdms2.open(cache.local_path, 'w')
                                    except cdms2.CDMSError as e:
                                        raise base.AccessError(cache.local_path, e)

                                try:
                                    data = infile(variable_name)
                                except Exception as e:
                                    raise base.WPSError('Error reading data from {url} error {error}', url=infile.id, error=e)

                                try:
                                    cache_obj.write(data, id=variable_name)
                                except Exception as e:
                                    logger.exception('%r', data)

                                    raise base.WPSError('Error writing data to cache file {url} error {error}', url=cache_obj.id, error=e)

                                try:
                                    outfile.write(data, id=variable_name)
                                except Exception as e:
                                    logger.exception('%r', data)

                                    raise base.WPSError('Error writing data to output file {url} error {error}', url=outfile.id, error=e)
                        except cdms2.CDMSError as e:
                            raise base.AccessError(chunk, e)
                except:
                    raise
                finally:
                    if cache_obj is not None:
                        cache_obj.close()
    except:
        raise
    finally:
        # Clean up the ingressed files
        for url, meta in ingress_map.iteritems():
            for chunk in meta['ingress_chunks']:
                os.remove(chunk)

    variable = cwt.Variable(output_url, variable_name)

    return { variable.name: variable.parameterize() }
