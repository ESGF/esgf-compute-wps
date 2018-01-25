import json
import os
import uuid

import cwt
from celery.utils.log import get_task_logger
from PyOphidia import client

from wps import settings
from wps import WPSError
from wps.tasks import base

__ALL__ = [
    'PROCESSES',
    'oph_submit',
]

logger = get_task_logger('wps.tasks.ophidia')

PROCESSES = {
    'Oph.max': 'max',
    'Oph.min': 'min',
    'Oph.avg': 'avg',
    'Oph.sum': 'sum',
    'Oph.std': 'std',
    'Oph.var': 'var',
}

class OphidiaTask(object):
    def __init__(self, name, operator, on_error=None):
        self.name = name
        self.operator = operator
        self.on_error = on_error
        self.arguments = []
        self.dependencies = []

    def add_arguments(self, **kwargs):
        self.arguments.extend(['{}={}'.format(key, value) for key, value in kwargs.iteritems()])

    def add_dependencies(self, *args):
        self.dependencies.extend(dict(task=x.name) for x in args)

    def to_dict(self):
        data = {
            'name': self.name,
            'operator': self.operator,
            'arguments': self.arguments,
        }

        if self.on_error:
            data['on_error'] = self.on_error

        if self.dependencies:
            data['dependencies'] = self.dependencies

        return data

class OphidiaWorkflow(object):
    def __init__(self, oph_client):
        self.oph_client = oph_client

        self.workflow = { 
            'name': 'ESGF WPS Workflow',
            'author': 'ESGF WPS',
            'abstract': 'Auto-generated abstract',
            'exec_mode': 'sync',
            'cwd': '/',
            'ncores': '2',
            'tasks': []
        }

    def add_tasks(self, *args):
        self.workflow['tasks'].extend(args)

    def check_error(self):
        if self.oph_client.last_error is not None and self.oph_client.last_error != '':
            error = '{}\n'.format(self.oph_client.last_error)

            res = self.oph_client.deserialize_response()
            
            try:
                for x in res['response'][2]['objcontent']:
                    for y in x['rowvalues']:
                        error += '\t{}: {}\n'.format(y[-3], y[-1])
            except IndexError:
                raise WPSError('Failed to parse last error from Ophidia')

            raise WPSError(error)

    def submit(self):
        self.check_error()

        self.oph_client.wsubmit(self.to_json())

    def to_json(self):
        def default(o):
            if isinstance(o, OphidiaTask):
                return o.to_dict()

        return json.dumps(self.workflow, default=default, indent=4)

@base.cwt_shared_task()
def oph_submit(self, parent_variables, variables, domains, operation, user_id, job_id):
    self.PUBLISH = base.ALL

    proc = process.Process(self.request.id)
    
    proc.initialize(user_id, job_id)

    v, d, o = self.load(parent_variables, variables, domains, operation)

    oph_client = client.Client(settings.OPH_USER, settings.OPH_PASSWORD, settings.OPH_HOST, settings.OPH_PORT)

    workflow = OphidiaWorkflow(oph_client)

    workflow.check_error()

    cores = o.get_parameter('cores')

    if cores is None:
        cores = settings.OPH_DEFAULT_CORES
    else:
        cores = cores.values[0]

    axes = o.get_parameter('axes')

    if axes is not None:
        axes = axes.values[0]
    else:
        axes = 'time'

    proc.log('Connected to Ophidia backend, building workflow')

    container_task = OphidiaTask('create container', 'oph_createcontainer', on_error='skip')
    container_task.add_arguments(container='work')

    proc.log('Add container task')

    # only take the first input
    inp = v.get(o.inputs[0])

    import_task = OphidiaTask('import data', 'oph_importnc')
    import_task.add_arguments(container='work', measure=inp.var_name, src_path=inp.uri, ncores=cores, imp_dim=axes)
    import_task.add_dependencies(container_task)

    proc.log('Added import task')

    try:
        operator = PROCESSES[o.identifier]
    except KeyError:
        raise WPSError('Process "{name}" does not exist for Ophidia backend', name=o.identifier)

    if axes == 'time':
        reduce_task = OphidiaTask('reduce data', 'oph_reduce')
        reduce_task.add_arguments(operation=operator, ncores=cores)
        reduce_task.add_dependencies(import_task)

        proc.log('Added reduction task over implicit axis')
    else:
        reduce_task = OphidiaTask('reduce data', 'oph_reduce2')
        reduce_task.add_arguments(operation=operator, dim=axes, ncores=cores)
        reduce_task.add_dependencies(import_task)

        proc.log('Added reduction task over axes "{}"', axes)

    output_name = '{}'.format(uuid.uuid4())

    export_task = OphidiaTask('export data', 'oph_exportnc2')
    export_task.add_arguments(output_path=settings.OPH_OUTPUT_PATH, output_name=output_name, ncores=cores, force='yes')
    export_task.add_dependencies(reduce_task)

    proc.log('Added export task')

    workflow.add_tasks(container_task, import_task, reduce_task, export_task)

    proc.log('Added tasks to workflow')

    workflow.submit()

    proc.log('Submitted workflow to Ophidia backend')

    workflow.check_error()

    proc.log('No errors reported by Ophidia')

    output_url = settings.OPH_OUTPUT_URL.format(output_path=settings.OPH_OUTPUT_PATH, output_name=output_name)

    output_var  = cwt.Variable(output_url, inp.var_name, name=o.name)

    return {o.name: output_var.parameterize()}
