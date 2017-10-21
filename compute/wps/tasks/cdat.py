#! /usr/bin/env python

import cwt
from celery.utils.log import get_task_logger

from wps import processes

logger = get_task_logger('wps.tasks.cdat')

@processes.cwt_shared_task()
def cache_variable(self, identifier, variables, domains, operations, **kwargs):
    self.PUBLISH = processes.RETRY | processes.FAILURE

    user, job = self.initialize(kwargs.get('user_id'), kwargs.get('job_id'), credentials=True)

    job.started()

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id(identifier, o)

    output_path = self.retrieve_variable([op.inputs[0]], op.domain, job, **kwargs)

    output_url = self.generate_output_url(output_path, **kwargs)

    op.inputs = [cwt.Variable(output_url, op.inputs[0].var_name)]

    op.parameters['axes'] = cwt.NamedParameter('axes', 'xy')

    data_inputs = cwt.WPS('').prepare_data_inputs(op, [], None)

    return data_inputs
