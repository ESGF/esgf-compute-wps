import os
import json

from celery.utils.log import get_task_logger
from PyOphidia import client

from wps import settings
from wps.tasks import process

__ALL__ = ['oph_submit']

logger = get_task_logger('wps.tasks.ophidia')

def get_linenumber():
    return '', ''

client.get_linenumber = get_linenumber

class OphidiaTask(object):
    def __init__(self, name, operator, **kwargs):
        self.name = name
        self.operator = operator
        self.on_error = kwargs.get('on_error', None)

        if self.on_error:
            del kwargs['on_error']

        self.dependencies = kwargs.get('dependencies', None)

        if self.dependencies:
            del kwargs['dependencies']

        self.args = kwargs

    def to_dict(self):
        data = {
            'name': self.name,
            'operator': self.operator,
            'arguments': ['{}={}'.format(key, value) for key, value in self.args.iteritems()]
        }

        if self.on_error:
            data['on_error'] = self.on_error

        if self.dependencies:
            data['dependencies'] = [dict(task=x.name) for x in self.dependencies]

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

    def check_error(self):
        if self.oph_client.last_error is not None and self.oph_client.last_error != '':
            logger.info(self.oph_client.last_response)

            raise Exception(self.oph_client.last_error)

    def add_task(self, name, operator, **kwargs):
        task = OphidiaTask(name, operator, **kwargs)

        self.workflow['tasks'].append(task)

        return task

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

    oph_createcontainer = workflow.add_task('create container', 'oph_createcontainer', on_error='skip', container='work', dim='lat|lon|time')

    oph_import = workflow.add_task('import data', 'oph_importnc', dependencies=[oph_createcontainer], container='work', exp_dim='lat|lon', imp_dim='time', measure=op.inputs[0].var_name, src_path=op.inputs[0].uri)

    oph_reduce = workflow.add_task('reduce', 'oph_reduce', dependencies=[oph_import], operation='max', dim='time')

    workflow.add_task('export', 'oph_exportnc2', force='yes', output_name='test', output_path='/wps')

    oph_client.wsubmit(workflow.to_json())

    workflow.check_error()
