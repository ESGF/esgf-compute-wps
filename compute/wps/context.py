import contextlib
import collections
import os
import re
import uuid
import urlparse
from collections import deque
from datetime import datetime

import cdms2
import cwt
import requests
from celery.utils.log import get_task_logger
from django.conf import settings

from wps import helpers
from wps import metrics
from wps import models
from wps import WPSError
from wps.tasks import credentials

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

        for name, value in data.iteritems():
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
        op_keys = self.operation.keys()

        adjacency = dict((x, dict((y, True if x in self.operation[y].inputs else
                             False) for y in op_keys)) for x in op_keys)

        sources = [x for x in op_keys if not any(adjacency[y][x] for y in op_keys)]

        others = [x for x in op_keys if x not in sources]

        self.output_id = [x for x in op_keys if not any(adjacency[x].values())]

        sorted = []

        while len(sources) > 0:
            item = sources.pop()

            sorted.append(self.operation[item])

            for name, connected in adjacency[item].iteritems():
                if connected:
                    sorted.append(self.operation[name])

                    index = others.index(name)

                    others.pop(index)

                    self.operation[name].add_parameters(intermediate='true')

        return deque(sorted)

    def add_output(self, operation):
        # Create a new variable, change the name to add some description
        name = '{!s}-{!s}'.format(operation.identifier, operation.name)

        output = operation.output

        variable = cwt.Variable(output.uri, output.var_name, name=name)

        # Add new variable to global dict
        #self.variable[operation.name] = variable

        if operation.name in self.output_id:
            # Add to output list
            self.output.append(variable)
        else:
            self.intermediate[operation.name] = variable

    def wait_operation(self, operation):
        result = operation.wait()

        if not result:
            raise WPSError('Operation {!r} failed', operation.identifier)

    def wait_for_inputs(self, operation):
        for input in operation.inputs:
            if input in self.state:
                executing_operation = self.state[input]

                self.wait_operation(executing_operation)

                self.add_output(executing_operation)

                # Remove completed operation from state
                del self.state[input]

    def wait_remaining(self):
        for operation in self.state.values():
            self.wait_operation(operation)

            self.add_output(operation)

            del self.state[operation.name]

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

        stat = os.stat(path)

        metrics.WPS_DATA_OUTPUT.inc(stat.st_size)

class VariableContext(object):
    def __init__(self, variable):
        self.variable = variable
        self.first = None
        self.units = None
        self.mapped = {}
        self.mapped_order = []
        self.cache = None
        self.chunk = []
        self.chunk_axis = None
        self.ingress = []
        self.process = []

    @staticmethod
    def load_value(obj, name, value):
        assert hasattr(obj, name)

        setattr(obj, name, value)

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
        variable = cwt.Variable.from_dict(data['variable'])

        obj = cls(variable)

        ignore = ['variable',]

        for name, value in data.iteritems():
            if name in ignore:
                continue

            cls.load_value(obj, name, value)

        cls.load_model(obj, 'cache', models.Cache)

        return obj

    @property
    def is_cached(self):
        return self.cache is not None

    def to_dict(self):
        data = self.__dict__.copy()

        data['variable'] = self.variable.parameterize()

        if data['cache'] is not None:
            data['cache'] = data['cache'].id

        return data

    def cache_mapped(self):
        return self.cache.localize_mapped(self.mapped)

    def check_access(self, cert=None):
        logger.info('Checking access to %r', self.variable.uri)

        logger.info('Certificate %r', cert)

        url = '{}.dds'.format(self.variable.uri)

        parts = urlparse.urlparse(url)

        try:
            response = requests.get(url, timeout=(2, 2), cert=cert,
                                    verify=False)
        except requests.ConnectTimeout:
            logger.exception('Timeout connecting to %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise WPSError('Timeout connecting to {!r}', parts.hostname)
        except requests.ReadTimeout:
            logger.exception('Timeout reading from %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise WPSError('Timeout reading from {!r}', parts.hostname)

        if response.status_code == 200:
            return True

        logger.info('Checking url failed with status code %r',
                    response.status_code)

        metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

        return False
    
    @contextlib.contextmanager
    def open(self, user):
        if user is not None and not self.check_access():
            cert_path = credentials.load_certificate(user)

            if not self.check_access(cert_path):
                raise WPSError('Cannot access file')

        try:
            with cdms2.open(self.variable.uri) as infile:
                logger.info('Opened %r', infile.id)

                yield infile[self.variable.var_name]
        except Exception:
            logger.exception('Failed to access file %r', self.variable.uri)

            parts = urlparse.urlparse(self.variable.uri)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise WPSError('Failed to access file {!r}', self.variable.uri)

    @contextlib.contextmanager
    def open_local(self, file_path):
        with cdms2.open(file_path) as infile:
            yield infile[self.variable.var_name]

    def generate_chunks(self, index):
        if index is None:
            return [x for x in range(len(self.chunk))]

        return [x for x in range(index, len(self.chunk),
                                 settings.WORKER_PER_USER)]

    def chunks_remote(self, index, context):
        mapped = self.mapped.copy()

        indices = self.generate_chunks(index)

        logger.info('Generating remote chunks')

        parts = urlparse.urlparse(self.variable.uri)

        with self.open(context.user) as variable:
            for index in indices:
                mapped.update({ self.chunk_axis: self.chunk[index] })

                with metrics.WPS_DATA_DOWNLOAD.labels(parts.hostname).time():
                    data = variable(**mapped)

                metrics.WPS_DATA_DOWNLOAD_BYTES.labels(parts.hostname,
                                                       self.variable.var_name).inc(data.nbytes)

                yield self.variable.uri, index, data

    def chunks_cache(self, index):
        mapped = helpers.decoder(self.cache.dimensions)

        indices = self.generate_chunks(index)

        logger.info('Generating cached chunks')

        with self.open_local(self.cache.local_path) as variable:
            for index in indices:
                mapped.update({ self.chunk_axis: self.chunk[index] })

                data = variable(**mapped)

                metrics.WPS_DATA_CACHE_READ.inc(data.nbytes)

                yield self.cache.local_path, index, data

        self.cache.accessed()

    def chunks_ingress(self, index):
        ingress = sorted(self.ingress)

        logger.info('Generating ingressed chunks')

        for index, ingress_path in enumerate(ingress):
            with self.open_local(ingress_path) as variable:
                yield ingress_path, index, variable()

    def chunks_process(self):
        process = sorted(self.process)

        logger.info('Generating procesed chunks')

        for index, process_path in enumerate(process):
            with self.open_local(process_path) as variable:
                yield process_path, index, variable()

    def chunks(self, index=None, context=None):
        if len(self.process) > 0:
            gen = self.chunks_process()
        elif len(self.ingress) > 0:
            gen = self.chunks_ingress(index)
        elif self.cache is not None:
            gen = self.chunks_cache(index)
        else:
            gen = self.chunks_remote(index, context)

        return gen
