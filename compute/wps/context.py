import contextlib
import collections
from datetime import datetime

import cdms2
import requests
import cwt
from celery.utils.log import get_task_logger

from wps import models
from wps import WPSError
from wps.tasks import credentials

logger = get_task_logger('wps.context')

class OperationContext(object):
    def __init__(self, inputs, domain, operation):
        self.inputs = inputs
        self.domain = domain
        self.operation = operation
        self.job = None
        self.user = None

    @classmethod
    def merge(cls, context):
        first = context[0]

        inputs = []

        for c in context:
            for i in c.inputs:
                if i.mapped is not None:
                    inputs.append(i)

        instance = cls(inputs, first.domain, first.operation)

        instance.job = first.job

        instance.user = first.user

        return instance

    @classmethod
    def from_dict(cls, data):
        inputs = [VariableContext.from_dict(x) for x in data['inputs']]

        if 'domain' in data:
            domain = cwt.Domain.from_dict(data['domain'])
        else:
            domain = None

        operation = cwt.Process.from_dict(data['operation'])

        obj = cls(inputs, domain, operation)

        if 'job_id' in data:
            try:
                obj.job = models.Job.objects.get(pk=data['job_id'])
            except models.Job.DoesNotExist:
                raise WPSError('Job {!r} does not exist', data['job_id'])

        if 'user_id' in data:
            try:
                obj.user = models.User.objects.get(pk=data['user_id'])
            except models.User.DoesNotExist:
                raise WPSError('User {!r} does not exist', data['user_id'])

        return obj

    @classmethod
    def from_data_inputs(cls, identifier, variable, domain, operation):
        try:
            target_op = [x for x in operation.values() if x.identifier ==
                         identifier][0]
        except IndexError:
            raise WPSError('Unable to find operation {!r}', identifier)

        target_domain = domain.get(target_op.domain, None)

        target_inputs = [VariableContext(variable[x]) for x in target_op.inputs]

        return cls(target_inputs, target_domain, target_op)

    def to_dict(self):
        data = {
            'inputs': [x.to_dict() for x in self.inputs],
            'operation': self.operation.parameterize(),
        }

        if self.domain is not None:
            data['domain'] = self.domain.parameterize()

        if self.job is not None:
            data['job_id'] = self.job.id

        if self.user is not None:
            data['user_id'] = self.user.id

        return data

    def input_set(self, indices):
        return [self.inputs[x] for x in indices]

class VariableContext(object):
    def __init__(self, variable):
        self.variable = variable
        self.mapped = None
        self.mapped_order = None
        self.cache_uri = None
        self.units = None
        self.first = None
        self.chunks = None

    @staticmethod
    def load_value(obj, name, data):
        if name in data:
            setattr(obj, name, data[name])

    @classmethod
    def from_dict(cls, data):
        variable = cwt.Variable.from_dict(data['variable'])

        obj = cls(variable)

        cls.load_value(obj, 'mapped', data)
        cls.load_value(obj, 'mapped_order', data)
        cls.load_value(obj, 'cache_uri', data)
        cls.load_value(obj, 'units', data)
        cls.load_value(obj, 'first', data)
        cls.load_value(obj, 'chunks', data)

        return obj

    def to_dict(self):
        data = {
            'variable': self.variable.parameterize(),
            'mapped': self.mapped,
            'mapped_order': self.mapped_order,
            'cache_uri': self.cache_uri,
            'units': self.units,
            'first': self.first,
            'chunks': self.chunks,
        }

        return data

    def check_access(self, cert=None):
        url = '{}.dds'.format(self.variable.uri)

        try:
            response = requests.get(url, timeout=(2, 2), cert=cert)
        except requests.ConnectTimeout:
            raise WPSError('Connection timeout')
        except requests.ReadTimeout:
            raise WPSError('Read timeout')

        return response.status_code == 200
    
    @contextlib.contextmanager
    def open(self, context):
        if not self.check_access():
            cert_path = credentials.load_certificate(context.user)

            if not self.check_access(cert_path):
                raise WPSError('Cannot access file')

        with cdms2.open(self.variable.uri) as infile:
            yield infile[self.variable.var_name]

    def get_units(self, context):
        with self.open(context) as infile:
            time = infile.getTime()

            self.units = time.units

            self.first = time[0]
