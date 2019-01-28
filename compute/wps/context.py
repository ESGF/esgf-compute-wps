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
        # TODO something other than a constant timeout
        try:
            result = operation.wait(timeout=10*60)
        except cwt.CWTError as e:
            raise WPSError('Process {!r} failed with {!r}', operation.identifier, str(e))

        if not result:
            raise WPSError('Operation {!r} failed', operation.identifier)

        self.job.step_complete()

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

        for operation in self.state.values():
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
        self.units = None
        self.output_data = None
        self.output_path = None
        self.grid = None
        self.gridder = None
        self.ignore = ('grid', 'gridder')

    @classmethod
    def merge_inputs(cls, contexts):
        logger.info('Merging %r contexts', len(contexts))

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
        try:
            first = contexts.pop()

            for index, input in enumerate(first.inputs):
                for context in contexts:
                    input.ingress.extend(context.inputs[index].ingress)

                    input.process.extend(context.inputs[index].process)
        except AttributeError:
            # Raised when contexts isn't a list
            first = contexts

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
        try:
            inputs = [VariableContext.from_dict(x) for x in data['inputs']]

            if 'domain' in data and data['domain'] is not None:
                domain = cwt.Domain.from_dict(data['domain'])
            else:
                domain = None

            operation = cwt.Process.from_dict(data['operation'])
        except:
            obj = cls()
        else:
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

        logger.info('Target operation %r', target_op)

        target_domain = domain.get(target_op.domain, None)

        logger.info('Target domain %r', target_domain)

        try:
            target_inputs = [VariableContext(variable[x]) for x in target_op.inputs]
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
    def is_compute(self):
        return self.operation.identifier not in ('CDAT.subset', 'CDAT.aggregate', 'CDAT.regrid')

    @property
    def is_regrid(self):
        return 'gridder' in self.operation.parameters

    def regrid_context(self, selector):
        if self.gridder is None:
            self.gridder = self.operation.get_parameter('gridder')

            self.grid = self.generate_grid(self.gridder, selector)

            if isinstance(self.gridder.grid, cwt.Variable):
                grid = self.gridder.grid.uri
            else:
                grid = self.gridder.grid

            metrics.WPS_REGRID.labels(self.gridder.tool, self.gridder.method, grid).inc()

        return self.grid, self.gridder.tool, self.gridder.method

    def sorted_inputs(self):
        units = set(x.units for x in self.inputs)

        if len(units) == 1:
            return sorted(self.inputs, key=lambda x: x.first)

        return sorted(self.inputs, key=lambda x: x.units)

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

    def parse_uniform_arg(self, value, default_start, default_n):
        result = re.match('^(\d\.?\d?)$|^(-?\d\.?\d?):(\d\.?\d?):(\d\.?\d?)$', value)

        if result is None:
            raise WPSError('Failed to parse uniform argument {value}', value=value)

        groups = result.groups()

        if groups[1] is None:
            delta = int(groups[0])

            default_n = default_n / delta
        else:
            default_start = int(groups[1])

            default_n = int(groups[2])

            delta = int(groups[3])

        start = default_start + (delta / 2.0)

        return start, default_n, delta

    def generate_selector(self, variable):
        """ Generates a selector for a variable.
        
        Iterates over the axis list and creates a dict selector for the 
        variabel.

        Args:
            variable: A cdms2.fvariable.FileVariable or cdms2.tvariable.TransientVariable.

        Returns:
            A dict keyed with the axis names and values of the axis endpoints as
            a tuple.
        """
        selector = {}

        for axis in variable.getAxisList():
            selector[axis.id] = (axis[0], axis[-1])

        return selector

    def subset_grid(self, grid, selector):
        target = cdms2.MV2.ones(grid.shape)

        target = target(**selector)

        target.setAxisList(grid.getAxisList())

        return target.getGrid()

    def generate_grid(self, gridder, selector):
        try:
            if isinstance(gridder.grid, cwt.Variable):
                grid = self.read_grid_from_file(gridder)
            else:
                grid = self.generate_user_defined_grid(gridder)
        except AttributeError:
            # Handle when gridder is None
            return None

        grid = self.subset_grid(grid, selector)

        return grid

    def read_grid_from_file(gridder):
        url_validator = URLValidator(['https', 'http'])

        try:
            url_validator(gridder.grid.uri)
        except ValidationError:
            raise WPSError('Path to grid file is not an OpenDAP url: {}', gridder.grid.uri)

        try:
            with cdms2.open(gridder.grid) as infile:
                data = infile(gridder.grid.var_name)
        except cdms2.CDMSError:
            raise WPSError('Failed to read the grid from {} in {}', gridder.grid.var_name, gridder.grid.uri)

        return data.getGrid()

    def generate_user_defined_grid(self, gridder):
        try:
            grid_type, grid_param = gridder.grid.split('~')
        except AttributeError:
            return None
        except ValueError:
            raise WPSError('Error generating grid "{name}"', name=gridder.grid)

        logger.info('Generating grid %r %r', grid_type, grid_param)

        if grid_type.lower() == 'uniform':
            result = re.match('^(.*)x(.*)$', grid_param)

            if result is None:
                raise WPSError('Failed to parse uniform configuration from {value}', value=grid_param)

            try:
                start_lat, nlat, delta_lat = self.parse_uniform_arg(result.group(1), -90.0, 180.0)
            except WPSError:
                raise

            try:
                start_lon, nlon, delta_lon = self.parse_uniform_arg(result.group(2), 0.0, 360.0)
            except WPSError:
                raise

            grid = cdms2.createUniformGrid(start_lat, nlat, delta_lat, start_lon, nlon, delta_lon)

            logger.info('Created target uniform grid {} from lat {}:{}:{} lon {}:{}:{}'.format(
                grid.shape, start_lat, delta_lat, nlat, start_lon, delta_lon, nlon))
        elif grid_type.lower() == 'gaussian':
            try:
                nlats = int(grid_param)
            except ValueError:
                raise WPSError('Error converting gaussian parameter to an int')

            grid = cdms2.createGaussianGrid(nlats)

            logger.info('Created target gaussian grid {}'.format(grid.shape))
        else:
            raise WPSError('Unknown grid type for regridding: {}', grid_type)

        return grid

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
    def filename(self):
        if self.variable is None:
            raise WPSError('No variable set')

        parts = urlparse.urlparse(self.variable.uri)

        return parts.path.split('/')[-1]

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
            response = requests.get(url, timeout=(2, 30), cert=cert,
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
                pass

        try:
            with cdms2.open(self.variable.uri) as infile:
                logger.info('Opened %r', infile.id)

                yield infile[self.variable.var_name]
        except WPSError:
            raise
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

    def write_cache_segment(self, input_index, index, context, data):
        if input_index is None:
            input_index = 0

        if index is None:
            index = 0

        ingress_filename = '{}_{:08}_{:08}.nc'.format(str(context.job.id), input_index, index)

        ingress_path = context.gen_ingress_path(ingress_filename)

        with cdms2.open(ingress_path, 'w') as outfile:
            outfile.write(data, id=self.variable.var_name)

        self.ingress.append(ingress_path)

    def chunks_remote(self, input_index, index, context):
        mapped = self.mapped.copy()

        indices = self.generate_chunks(index)

        logger.info('Generating remote chunks for %r', index)

        logger.info('Indices %r chunks %r', indices, self.chunk)

        parts = urlparse.urlparse(self.variable.uri)

        with self.open(context.user) as variable:
            for chunk_index in indices:
                mapped.update({ self.chunk_axis: self.chunk[chunk_index] })

                logger.info('Reading %r %r', mapped, self.chunk[chunk_index])

                with metrics.WPS_DATA_DOWNLOAD.labels(parts.hostname).time():
                    data = variable(**mapped)

                metrics.WPS_DATA_DOWNLOAD_BYTES.labels(parts.hostname,
                                                       self.variable.var_name).inc(data.nbytes)

                if not settings.INGRESS_ENABLED:
                    self.write_cache_segment(input_index, index, context, data)

                yield self.variable.uri, chunk_index, data

    def chunks_cache(self, index):
        mapped = self.cache_mapped()

        indices = self.generate_chunks(index)

        logger.info('Generating cached chunks for %r', index)

        logger.info('Indices %r chunks %r', indices, self.chunk)

        with self.open_local(self.cache.local_path) as variable:
            for chunk_index in indices:
                mapped.update({ self.chunk_axis: self.chunk[chunk_index] })

                logger.info('Reading %r %r', mapped, self.chunk[chunk_index])

                data = variable(**mapped)

                metrics.WPS_DATA_CACHE_READ.inc(data.nbytes)

                yield self.cache.local_path, chunk_index, data

        self.cache.accessed()

    def chunks_ingress(self, index):
        ingress = sorted(self.ingress)

        logger.info('Generating ingressed chunks')

        for chunk_index, ingress_path in enumerate(ingress):
            with self.open_local(ingress_path) as variable:
                yield ingress_path, chunk_index, variable()

    def chunks_process(self):
        process = sorted(self.process)

        logger.info('Generating procesed chunks')

        for index, process_path in enumerate(process):
            with self.open_local(process_path) as variable:
                yield process_path, index, variable()

    def chunks(self, input_index=None, index=None, context=None):
        if len(self.process) > 0:
            gen = self.chunks_process()
        elif len(self.ingress) > 0:
            gen = self.chunks_ingress(index)
        elif self.cache is not None:
            gen = self.chunks_cache(index)
        else:
            gen = self.chunks_remote(input_index, index, context)

        return gen
