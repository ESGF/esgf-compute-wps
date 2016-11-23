import json
import re

class OphidiaTask(object):
    def __init__(self, name, operator, **kwargs):
        on_error = kwargs.pop('on_error', None)

        self._doc = {
            'name': name,
            'operator': operator,
            'arguments': ['%s=%s' %
                          (k, self._convert(v)) for k, v in kwargs.iteritems()]
        }

        if on_error:
            self._doc['on_error'] = on_error

    @property
    def name(self):
        return self._doc['name']

    @property
    def operator(self):
        return self._doc['operator']

    @property
    def doc(self):
        return self._doc

    def _convert(self, value):
        if type(value) == bool:
            if value:
                return 'yes'
            else:
                return 'no'
        elif (isinstance(value, OphidiaTask) and
              value.operator == 'oph_createcontainer'):
            return value.argument('container')

        return value

    def argument(self, name):
        pattern = '%s=(.*)' % (name, )

        print pattern

        for arg in self._doc['arguments']:
            match = re.match(pattern, arg)

            if match:
                return match.group(1)

        return None

    def depends(self, dependency, dep_type=None):
        dep = {
            'task': dependency.name
        }

        if dep_type:
            dep['type'] = dep_type

        if 'dependencies' not in self._doc:
            self._doc['dependencies'] = []

        self._doc['dependencies'].append(dep)

class OphidiaWorkflowBuilder(object):
    def __init__(self, cores):
        self._doc = {
            'name': 'wps',
            'author': 'wps_cwt',
            'abstract': 'wps generated',
            'exec_mode': 'sync',
            'cwd': '/',
            'ncores': str(cores),
            'on_exit': 'oph_delete',
            'tasks': [

            ]
        }

        self._tasks = []

    @property
    def to_json(self):
        for task in self._tasks:
            self._doc['tasks'].append(task.doc)
    
        return json.dumps(self._doc)

    def create_task(self, name, operator, **kwargs):
        task = OphidiaTask(name, operator, **kwargs)

        self._tasks.append(task)

        return task
