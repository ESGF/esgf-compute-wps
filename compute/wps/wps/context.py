from __future__ import division
from future import standard_library
standard_library.install_aliases() # noqa
from builtins import str
from builtins import object
import os
import uuid
from collections import deque

import cwt
from celery.utils.log import get_task_logger
from django.conf import settings

from wps import models
from wps import WPSError

logger = get_task_logger('wps.context')


class WorkflowOperationContext(object):
    def __init__(self, variable, domain, operation):
        self.variable = variable
        self.domain = domain
        self.operation = operation
        self.job = None
        self.user = None
        self.process = None
        self.output = []
        self.intermediate = {}
        self.output_id = []
        self.state = {}

    @staticmethod
    def load_model(obj, name, model_class):
        assert hasattr(obj, name)

        pk = getattr(obj, name, None)

        if pk is not None:
            try:
                value = model_class.objects.get(pk=pk)
            except model_class.DoesNotExist:
                raise WPSError('{!s} {!r} does not exist', model_class.__name__, pk)

            setattr(obj, name, value)

    @classmethod
    def from_data_inputs(cls, variable, domain, operation):
        instance = cls(variable, domain, operation)

        return instance

    @classmethod
    def from_dict(cls, data):
        variable = data['variable']

        domain = data['domain']

        operation = data['operation']

        instance = cls(variable, domain, operation)

        ignore = ['variable', 'domain', 'operation', 'state']

        for name, value in list(data.items()):
            if name in ignore:
                continue

            assert hasattr(instance, name)

            setattr(instance, name, value)

        cls.load_model(instance, 'job', models.Job)

        cls.load_model(instance, 'user', models.User)

        cls.load_model(instance, 'process', models.Process)

        return instance

    def to_dict(self):
        data = self.__dict__.copy()

        data['job'] = data['job'].id

        data['user'] = data['user'].id

        data['process'] = data['process'].id

        return data

    def build_execute_graph(self):
        op_keys = list(self.operation.keys())

        adjacency = dict((x, dict((y, True if x in self.operation[y].inputs else
                                   False) for y in op_keys)) for x in op_keys)

        sources = [x for x in op_keys if not any(adjacency[y][x] for y in op_keys)]

        others = [x for x in op_keys if x not in sources]

        self.output_id = [x for x in op_keys if not any(adjacency[x].values())]

        sorted = []

        while len(sources) > 0:
            item = sources.pop()

            sorted.append(self.operation[item])

            for name, connected in list(adjacency[item].items()):
                if connected:
                    sorted.append(self.operation[name])

                    index = others.index(name)

                    others.pop(index)

                    self.operation[name].add_parameters(intermediate='true')

        return deque(sorted)

    def to_operation_context(self, process, extra):
        inputs = []
        to_process = process.inputs.copy()

        for x in process.inputs:
            try:
                inputs.append(self.variable[x])
            except KeyError:
                pass
            else:
                to_process.pop(to_process.index(x))

        for x in process.inputs:
            try:
                inputs.append(extra[x])
            except KeyError:
                pass
            else:
                to_process.pop(to_process.index(x))

        if len(to_process) > 0:
            raise WPSError('Did not resolve all inputs for {!r}', process.identifier)

        domain = self.domain.get(process.domain, None)

        op = OperationContext(inputs, domain, process)

        op.user = self.user

        op.job = self.job

        return op

    def can_compute_local(self, process):
        # TODO add some logic to determine if computing locally is optimal
        return True

    def where_to_compute(self, process):
        # TODO add some logic to determine where to execute
        return cwt.WPSClient(settings.WPS_ENDPOINT, api_key=self.user.auth.api_key)

    def add_output(self, operation):
        # Create a new variable, change the name to add some description
        name = '{!s}-{!s}'.format(operation.identifier, operation.name)

        output = operation.output

        variable = cwt.Variable(output.uri, output.var_name, name=name)

        # Add new variable to global dict
        # self.variable[operation.name] = variable

        if operation.name in self.output_id:
            # Add to output list
            self.output.append(variable)
        else:
            self.intermediate[operation.name] = variable

    def wait_operation(self, operation):
        # TODO something other than a constant timeout
        try:
            result = operation.wait(timeout=10*60)
        except cwt.CWTError as e:
            raise WPSError('Process {!r} failed with {!r}', operation.identifier, str(e))

        if not result:
            raise WPSError('Operation {!r} failed', operation.identifier)

    def wait_for_inputs(self, operation):
        completed = []

        for input in operation.inputs:
            if input in self.state:
                executing_operation = self.state.pop(input)

                self.wait_operation(executing_operation)

                self.add_output(executing_operation)

                completed.append(executing_operation)

        return completed

    def wait_remaining(self):
        completed = []

        for operation in list(self.state.values()):
            self.wait_operation(operation)

            self.add_output(operation)

            del self.state[operation.name]

            completed.append(operation)

        return completed

    def add_executing(self, operation):
        self.state[operation.name] = operation

    def get_input(self, name):
        if name in self.variable:
            return self.variable[name]
        elif name in self.intermediate:
            return self.intermediate[name]

        raise WPSError('Unable to locate input {!r}', name)

    def prepare(self, operation):
        operation.inputs = [self.get_input(x) for x in operation.inputs]

        operation.domain = self.domain.get(operation.domain, None)

        if 'domain' in operation.parameters:
            del operation.parameters['domain']


class OperationContext(object):
    def __init__(self, inputs=None, domain=None, operation=None):
        self.inputs = inputs
        self.domain = domain
        self.operation = operation
        self.job = None
        self.user = None
        self.process = None
        self.output_path = None
        self.output_data = None
        self.grid = None
        self.gridder = None
        self.ignore = ('grid', 'gridder')

    @staticmethod
    def load_model(obj, name, model_class):
        assert hasattr(obj, name)

        pk = getattr(obj, name, None)

        if pk is not None:
            try:
                value = model_class.objects.get(pk=pk)
            except model_class.DoesNotExist:
                raise WPSError('{!s} {!r} does not exist', model_class.__name__, pk)

            setattr(obj, name, value)

    @classmethod
    def from_dict(cls, data):
        try:
            inputs = [cwt.Variable.from_dict(x) for x in data['inputs']]

            if 'domain' in data and data['domain'] is not None:
                domain = cwt.Domain.from_dict(data['domain'])
            else:
                domain = None

            operation = cwt.Process.from_dict(data['operation'])
        except Exception:
            obj = cls()
        else:
            obj = cls(inputs, domain, operation)

        ignore = ['inputs', 'domain', 'operation']

        for name, value in list(data.items()):
            if name in ignore:
                continue

            if hasattr(obj, name):
                setattr(obj, name, data[name])

        cls.load_model(obj, 'job', models.Job)

        cls.load_model(obj, 'user', models.User)

        cls.load_model(obj, 'process', models.Process)

        return obj

    @classmethod
    def from_data_inputs(cls, identifier, variable, domain, operation):
        try:
            target_op = [x for x in list(operation.values()) if x.identifier ==
                         identifier][0]
        except IndexError:
            raise WPSError('Unable to find operation {!r}', identifier)

        logger.info('Target operation %r', target_op)

        target_domain = domain.get(target_op.domain, None)

        logger.info('Target domain %r', target_domain)

        try:
            target_inputs = [variable[x] for x in target_op.inputs]
        except KeyError as e:
            raise WPSError('Missing variable with name {!r}', str(e))

        logger.info('Target inputs %r', target_inputs)

        return cls(target_inputs, target_domain, target_op)

    def to_dict(self):
        data = self.__dict__.copy()

        for x in self.ignore:
            if x in data:
                del data[x]

        if data['inputs'] is not None:
            data['inputs'] = [x.to_dict() for x in data['inputs']]

        if data['operation'] is not None:
            data['operation'] = data['operation'].parameterize()

        if data['domain'] is not None:
            data['domain'] = data['domain'].parameterize()

        if data['job'] is not None:
            data['job'] = data['job'].id

        if data['user'] is not None:
            data['user'] = data['user'].id

        if data['process'] is not None:
            data['process'] = data['process'].id

        return data

    @property
    def is_regrid(self):
        return 'gridder' in self.operation.parameters

    def gen_public_path(self):
        filename = '{}.nc'.format(uuid.uuid4())

        base_path = os.path.join(settings.WPS_PUBLIC_PATH, str(self.user.id),
                                 str(self.job.id))

        if not os.path.exists(base_path):
            os.makedirs(base_path)

        return os.path.join(base_path, filename)
