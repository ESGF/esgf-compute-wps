#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import json
import uuid
from collections import deque
from contextlib import closing

import cdms2
import cwt
import requests
from celery import group
from celery import shared_task
from celery.utils.log import get_task_logger

from wps import models
from wps import node_manager
from wps import settings

logger = get_task_logger(__name__)

dummy_wps = cwt.WPS('')

@shared_task()
def workflow(data_inputs):
    operations, domains, inputs = cwt.WPS.parse_data_inputs(data_inputs)

    find_by_id = lambda x: [y for y in operations if y.identifier == x][0]

    workflow = find_by_id('CDAT.workflow')

    temp_inputs = dict((x.name, x) for x in inputs)

    temp_operations = dict((x.name, x) for x in operations)

    workflow.resolve_inputs(temp_inputs, temp_operations)

    logger.info('Workflow inputs {}'.format(workflow.inputs))

    child_tasks = []

    servers = models.Server.objects.all()

    idx = 0

    for p in workflow.inputs:
        new_data_inputs = dummy_wps.prepare_data_inputs(p, p.inputs, domains)
    
        if idx >= len(servers):
            idx = 0

        s = servers[idx]

        idx += 1

        if p.identifier == 'CDAT.avg':
            if s.host == 'default':
                child_tasks.append(avg.s(new_data_inputs, p.name))
            else:
                child_tasks.append(remote.s(s.id, new_data_inputs, p.name))

    output = []

    if len(child_tasks) > 0:
        logger.info('Calling child tasks {}'.format(child_tasks))

        child_group = group(child_tasks)()

        import time

        while not child_group.ready():
            time.sleep(1)

        for c in child_group:
            file_name = c.result.split('/')[-1]

            local_file_path = '{}/{}'.format(settings.OUTPUT_LOCAL_PATH, file_name)

            logger.info('Localizing result {}'.format(c.result))

            response = requests.get(c.result)

            with open(local_file_path, 'w') as outfile:
                for chunk in response.iter_content():
                    outfile.write(chunk)

            if settings.DAP:
                url = settings.DAP_URL.format(file_name=file_name)
            else:
                url = settings.OUTPUT_URL.format(file_name=file_name)

            logger.info('Output url {}'.format(url))
            
            output.append(url)

    return output

@shared_task()
def remote(server_id, data_inputs, result):
    try:
        server = models.Server.objects.get(pk=server_id)
    except models.Server.DoesNotExist:
        raise Exception('Server does not exist {}'.format(server_id))

    logger.info('Sending remote request to {}'.format(server.host))

    operations, domains, inputs = cwt.WPS.parse_data_inputs(data_inputs)

    find_by_name = lambda x: [y for y in operations if y.name == x][0]

    op = find_by_name(result)

    temp_inputs = dict((x.name, x) for x in inputs)

    temp_operations = dict((x.name, x) for x in operations)

    op.resolve_inputs(temp_inputs, temp_operations)

    logger.info('Remove inputs {}'.format(op.inputs))

    wps = cwt.WPS(server.host)

    for p in wps.processes():
        logger.info(p.identifier)

    wps.execute(op, inputs=op.inputs)

    import time

    while op.processing:
        logger.info(op.status)

        time.sleep(1)

    logger.info(op.status)

    output = json.loads(op.output[0].data.value)

    return output

@shared_task()
def avg(data_inputs, result=None):
    operations, domains, inputs = cwt.WPS.parse_data_inputs(data_inputs)

    find_by_name = lambda x: [y for y in operations if y.name == x][0]

    if result is not None:
        op = find_by_name(result)
    else:
        op = operations[0]

    temp_inputs = dict((x.name, x) for x in inputs)

    temp_operations = dict((x.name, x) for x in operations)

    op.resolve_inputs(temp_inputs, temp_operations)

    if len(op.inputs) < 1:
        raise Exception('Expecting a single input file')

    logger.info('AVG inputs {}'.format(op.inputs))

    child_tasks = []

    # Normally the node_manager would make the decision of which server to execute on
    servers = models.Server.objects.all()

    idx = 0

    manager = node_manager.NodeManager()

    for p in op.inputs:
        if not isinstance(p, cwt.Variable):
            new_data_inputs = dummy_wps.prepare_data_inputs(p, p.inputs, domains)

            if idx >= len(servers):
                idx = 0

            s = servers[idx]

            idx += 1

            logger.info('SERVER {}'.format(s.host))

            if p.identifier == 'CDAT.avg':
                if s.host == 'default':
                    child_tasks.append(avg.s(new_data_inputs))

                    manager.create_job(s)
                else:
                    child_tasks.append(remote.s(s.id, data_inputs, p.name))

    input_data = [x.uri for x in op.inputs if isinstance(x, cwt.Variable)]

    if len(child_tasks) > 0:
        logger.info('Calling children')

        child_group = group(child_tasks)()

        import time

        while not child_group.ready():
            time.sleep(1)

        for c in child_group:
            file_name = c.result.split('/')[-1]

            local_file_path = '{}/{}'.format(settings.OUTPUT_LOCAL_PATH, file_name)

            logger.info('Localizing result {}'.format(c.result))

            response = requests.get(c.result)

            if response.status_code != 200:
                raise Exception('Failed to download file, status_code {}'.format(response.status_code))

            with open(local_file_path, 'w') as outfile:
                for chunk in response.iter_content():
                    outfile.write(chunk)

            input_data.append('file://{}'.format(local_file_path))

    with closing(cdms2.open(input_data[0], 'r')) as f:
        logger.info('Reading data from %s', input_data[0])        

        var_name = 'tas'
        #var_name = op.inputs[0].var_name

        n = f[var_name].shape[0]

    file_name = '{}.nc'.format(uuid.uuid4())

    file_path = '{0}/{1}'.format(settings.OUTPUT_LOCAL_PATH, file_name)

    logger.info('Local file path {}'.format(file_path))

    logger.info('Averaging files {}'.format(input_data))

    inps = [cdms2.open(x, 'r') for x in input_data]

    n_inps = len(inps)

    with closing(cdms2.open(file_path, 'w')) as fout:
        for i in xrange(0, n, 200):
            for idx, v in enumerate(inps):
                data = v(var_name, slice(i, i+200))

                if idx == 0:
                    out = data
                else:
                    out[:] += data[:]

            out /= n_inps

            fout.write(out, id=var_name)

    if settings.DAP:
        url = settings.DAP_URL.format(file_name=file_name)
    else:
        url = settings.OUTPUT_URL.format(file_name=file_name)

    logger.info('Output url {}'.format(url))

    return url
