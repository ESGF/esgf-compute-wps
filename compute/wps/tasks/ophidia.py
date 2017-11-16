import json
import os
import uuid

from celery.utils.log import get_task_logger
from PyOphidia import client

from wps import settings
from wps.tasks import process

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
            except:
                pass

            raise Exception(error)

    def submit(self):
        self.check_error()

        self.oph_client.wsubmit(self.to_json())

    def to_json(self):
        def default(o):
            if isinstance(o, OphidiaTask):
                return o.to_dict()
            return None

        return json.dumps(self.workflow, default=default, indent=4)

@process.cwt_shared_task()
def oph_submit(self, data_inputs, identifier, **kwargs):
    self.PUBLISH = process.ALL

    v, d, o = self.load_data_inputs(data_inputs)

    op = self.op_by_id(identifier, o)

    oph_client = client.Client(settings.OPH_USER, settings.OPH_PASSWORD, settings.OPH_HOST, settings.OPH_PORT)

    workflow = OphidiaWorkflow(oph_client)

    workflow.check_error()

    container_task = OphidiaTask('create container', 'oph_createcontainer', on_error='skip')
    container_task.add_arguments(container='work')

    import_task = OphidiaTask('import data', 'oph_importnc')
    import_task.add_arguments(container='work', measure=op.inputs[0].var_name, src_path=op.inputs[0].uri)
    import_task.add_dependencies(container_task)

    try:
        operator = PROCESSES[identifier]
    except KeyError:
        raise Exception('Process "{}" does not exist for Ophidia backend'.format(identifier))

    axes = op.get_parameter('axes')

    if axes is None:
        reduce_task = OphidiaTask('reduce data', 'oph_reduce')
        reduce_task.add_arguments(operation=operator)
        reduce_task.add_dependencies(import_task)
    else:
        reduce_task = OphidiaTask('reduce data', 'oph_reduce2')
        reduce_task.add_arguments(operation=operator, dim=axes.values[0])
        reduce_task.add_dependencies(import_task)

    output_path = '/wps/output'
    output_name = '{}'.format(uuid.uuid4())

    export_task = OphidiaTask('export data', 'oph_exportnc')
    export_task.add_arguments(output_path=output_path, output_name=output_name)
    export_task.add_dependencies(reduce_task)

    workflow.add_tasks(container_task, import_task, reduce_task, export_task)

    workflow.submit()

    workflow.check_error()

    output_url = settings.OPH_OUTPUT_URL.format(output_path=output_path, output_name=output_name)

    variable = cwt.Variable(output_url, op.inputs[0].var_name)

    return variable.parameterize()
