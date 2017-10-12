#! /usr/bin/env python

import cdms2
import cwt
from celery.utils.log import get_task_logger

from wps import processes

logger = get_task_logger('wps.tasks.cdat')

@processes.cwt_shared_task()
def cache_variable(self, identifier, variables, domains, operations, **kwargs):
    self.PUBLISH = processes.RETRY | processes.FAILURE

    job, status = self.initialize(credentials=True, **kwargs)

    variables, domains, operations = self.load(variables, domains, operations)

    logger.info(variables)
    logger.info(domains)
    logger.info(operations)

    op = self.op_by_id(identifier, operations)

    var_name = reduce(lambda x, y: x if x == y else None, [x.var_name for x in op.inputs])

    out_local_path = self.generate_local_output()

    grid, tool, method = self.generate_grid(op, variables, domains)

    with cdms2.open(out_local_path, 'w') as out:
        def read_callback(data):
            if grid is not None:
                data = data.regrid(grid, regridTool=tool, regridMethod=method)

            out.write(data, id=var_name)

        self.cache_multiple_input(op.inputs, op.domain, status, read_callback)

    out_path = self.generate_output(out_local_path, **kwargs)

    out_var = cwt.Variable(out_path, var_name)

    op.inputs = [out_var]

    data_inputs = cwt.WPS('').prepare_data_inputs(op, [], None)

    return data_inputs
