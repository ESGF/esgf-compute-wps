from __future__ import division
from future import standard_library
standard_library.install_aliases() # noqa
from builtins import str
from builtins import object
import os
import uuid

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

        for x in operation.values():
            inputs = []

            for y in x.inputs:
                if y in variable:
                    inputs.append(variable[y])
                elif y in operation:
                    inputs.append(operation[y])
                else:
                    raise WPSError('Failed to resolve input {!r}', y)

            x.inputs = inputs

            try:
                x.domain = domain[x.domain]
            except KeyError:
                pass

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

    def output_ops(self):
        out_deg = dict((x, self.node_out_deg(x)) for x in self.operation.keys())

        return [self.operation[x] for x, y in out_deg.items() if y == 0]

    def node_in_deg(self, node):
        return len([x for x in node.inputs if x.name in self.operation])

    def node_out_deg(self, node):
        return len([x for x, y in self.operation.items() if any(node == z.name for z in y.inputs)])

    def find_neighbors(self, node):
        return [x for x, y in self.operation.items() if node in [x.name for x in y.inputs]]

    def topo_sort(self):
        in_deg = dict((x, self.node_in_deg(y)) for x, y in self.operation.items())

        neigh = dict((x, self.find_neighbors(x)) for x in in_deg.keys())

        queue = [x for x, y in in_deg.items() if y == 0]

        cnt = 0

        topo_order = []

        while queue:
            next = queue.pop(0)

            topo_order.append(self.operation[next])

            for x in neigh[next]:
                in_deg[x] -= 1

                if in_deg[x] == 0:
                    queue.append(x)

            cnt += 1

        if cnt != len(self.operation):
            raise WPSError('Failed to compute the graph')

        return topo_order

    def build_output_variable(self, var_name, name=None):
        local_path = self.generate_local_path()

        relpath = os.path.relpath(local_path, settings.WPS_PUBLIC_PATH)

        url = settings.WPS_DAP_URL.format(filename=relpath)

        self.output.append(cwt.Variable(url, var_name, name=name).to_dict())

        return local_path

    def generate_local_path(self):
        filename = '{}.nc'.format(uuid.uuid4())

        base_path = os.path.join(settings.WPS_PUBLIC_PATH, str(self.user.id),
                                 str(self.job.id))

        if not os.path.exists(base_path):
            os.makedirs(base_path)

        return os.path.join(base_path, filename)


class OperationContext(object):
    def __init__(self, inputs=None, domain=None, operation=None):
        self.inputs = inputs
        self.domain = domain
        self.operation = operation
        self.job = None
        self.user = None
        self.process = None
        self.output = []

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

    def build_output_variable(self, var_name, name=None):
        local_path = self.generate_local_path()

        relpath = os.path.relpath(local_path, settings.WPS_PUBLIC_PATH)

        url = settings.WPS_DAP_URL.format(filename=relpath)

        self.output.append(cwt.Variable(url, var_name, name=name).to_dict())

        return local_path

    def generate_local_path(self):
        filename = '{}.nc'.format(uuid.uuid4())

        base_path = os.path.join(settings.WPS_PUBLIC_PATH, str(self.user.id),
                                 str(self.job.id))

        if not os.path.exists(base_path):
            os.makedirs(base_path)

        return os.path.join(base_path, filename)
