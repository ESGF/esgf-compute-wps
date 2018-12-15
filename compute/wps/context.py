import contextlib
import collections
import os
import uuid
from datetime import datetime

import cdms2
import cwt
import requests
from celery.utils.log import get_task_logger
from django.conf import settings

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
        self.process = None
        self.units = None
        self.output_path = None

    @classmethod
    def merge_inputs(cls, contexts):
        first = contexts[0]

        inputs = []

        for context in contexts:
            inputs.extend(context.inputs)

        instance = cls(inputs, first.domain, first.operation)

        instance.units = first.units

        instance.output_path = first.output_path

        instance.job = first.job

        instance.user = first.user

        instance.process = first.process

        return instance

    @classmethod
    def merge_ingress(cls, contexts):
        first = contexts.pop()

        for index, input in enumerate(first.inputs):
            for context in contexts:
                input.ingress.extend(context.inputs[index].ingress)

                input.process.extend(context.inputs[index].process)

        return first

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
        inputs = [VariableContext.from_dict(x) for x in data['inputs']]

        if 'domain' in data and data['domain'] is not None:
            domain = cwt.Domain.from_dict(data['domain'])
        else:
            domain = None

        operation = cwt.Process.from_dict(data['operation'])

        obj = cls(inputs, domain, operation)

        ignore = ['inputs', 'domain', 'operation']

        for name, value in data.iteritems():
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
            target_op = [x for x in operation.values() if x.identifier ==
                         identifier][0]
        except IndexError:
            raise WPSError('Unable to find operation {!r}', identifier)

        target_domain = domain.get(target_op.domain, None)

        target_inputs = [VariableContext(variable[x]) for x in target_op.inputs]

        return cls(target_inputs, target_domain, target_op)

    def to_dict(self):
        data = self.__dict__.copy()

        data['inputs'] = [x.to_dict() for x in data['inputs']]

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

    def sorted_inputs(self):
        units = set(x.units for x in self.inputs)

        if len(units) == 1:
            return sorted(self.inputs, key=lambda x: x.units)

        return sorted(self.inputs, key=lambda x: x.first)

    def gen_public_path(self):
        filename = '{}.nc'.format(uuid.uuid4())

        return os.path.join(settings.WPS_PUBLIC_PATH, str(self.user.id),
                            str(self.job.id), filename)

    def gen_ingress_path(self, filename):
        return os.path.join(settings.WPS_INGRESS_PATH, filename)

    @contextlib.contextmanager
    def new_output(self, path):
        base_path = os.path.dirname(path)

        try:
            os.makedirs(base_path)
        except OSError:
            pass

        with cdms2.open(path, 'w') as outfile:
            yield outfile

class VariableContext(object):
    def __init__(self, variable):
        self.variable = variable
        self.first = None
        self.units = None
        self.mapped = {}
        self.mapped_order = []
        self.cache_uri = None
        self.cache_mapped = {}
        self.chunk = []
        self.chunk_axis = None
        self.ingress = []
        self.process = []

    @staticmethod
    def load_value(obj, name, value):
        assert hasattr(obj, name)

        setattr(obj, name, value)

    @classmethod
    def from_dict(cls, data):
        variable = cwt.Variable.from_dict(data['variable'])

        obj = cls(variable)

        ignore = ['variable']

        for name, value in data.iteritems():
            if name in ignore:
                continue

            cls.load_value(obj, name, value)

        return obj

    def to_dict(self):
        data = self.__dict__.copy()

        data['variable'] = self.variable.parameterize()

        return data

    @property
    def is_cached(self):
        return self.cache_uri is not None

    @property
    def is_ingressed(self):
        return len(self.ingress) > 0

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
        if context is not None and not self.check_access():
            cert_path = credentials.load_certificate(context.user)

            if not self.check_access(cert_path):
                raise WPSError('Cannot access file')

        with cdms2.open(self.variable.uri) as infile:
            logger.info('Opened %r', infile.id)

            yield infile[self.variable.var_name]

    @contextlib.contextmanager
    def open_local(self, file_path):
        with cdms2.open(file_path) as infile:
            yield infile[self.variable.var_name]

    def chunks_remote(self, context, indices):
        logger.info('Generating chunks from remote source')

        if indices is None:
            indices = range(self.chunk)

        mapped = self.mapped.copy()

        with self.open(context) as variable:
            for index in indices:
                mapped.update({ self.chunk_axis: self.chunk[index] })

                yield index, variable(**mapped)

    def chunks_cache(self, context, indices):
        logger.info('Generating chunks from cache source')

        if indices is None:
            indices = range(self.chunk)

        mapped = self.cache_mapped.copy()

        with self.open_local(self.cache_uri) as variable:
            logger.info('Opening %r %r', self.cache_uri, variable.shape)

            for index in indices:
                mapped.update({ self.chunk_axis: self.chunk[index] })

                logger.info('Read chunk %r', mapped)

                yield index, variable(**mapped)

    def chunks_ingress(self, indices):
        logger.info('Generating chunks from ingressed source')

        if indices is None:
            indices = range(len(self.ingress))

        ingress = sorted(self.ingress)

        for index in indices:
            with self.open_local(ingress[index]) as variable:
                logger.info('Opening %r %r', variable.id, variable.shape)

                yield index, variable()

    def chunks_process(self, indices):
        logger.info('Generating chunks from process output')

        if indices is None:
            indices = range(len(self.process))

        process = sorted(self.process)

        for index in indices:
            with self.open_local(process[index]) as variable:
                logger.info('Opening %r %r', variable.id, variable.shape)

                yield index, variable()

    def chunks(self, context=None, indices=None):
        if len(self.process) > 0:
            gen = self.chunks_process(indices)
        elif self.cache_uri is not None:
            gen = self.chunks_cache(context, indices)
        elif len(self.ingress) > 0:
            gen = self.chunks_ingress(indices)
        else:
            gen = self.chunks_remote(context, indices)

        return gen
