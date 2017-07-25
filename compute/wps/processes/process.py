#! /usr/bin/env python

import collections
import copy
import hashlib
import json
import os
import uuid
from contextlib import closing
from contextlib import nested
from functools import partial

import cdms2
import celery
import cwt
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings as global_settings
from cwt.wps_lib import metadata

from wps import models
from wps import wps_xml
from wps import settings

__all__ = [
    'REGISTRY',
    'register_process',
    'get_process',
    'CWTBaseTask', 
    'Status',
    'handle_output',
    'cwt_shared_task',
]

logger = get_task_logger('wps.processes.process')

REGISTRY = {}

class AccessError(Exception):
    pass

class InvalidShapeError(Exception):
    pass

def get_process(name):
    """ Returns a process.

    Retrieves a process from a global registry.

    Args:
        name: A string name of the process.

    Returns:
        A method that has been wrapped as a celery task.

    Raises:
        Exception: A process of name does not exist.
    """
    try:
        return REGISTRY[name]
    except KeyError:
        raise Exception('Process {} does not exist'.format(name))

def register_process(name):
    """ Process decorator.

    Registers a process with the global dictionary.

    @register_process('Demo')
    def demo(a, b, c):
        return a+b+c

    Args:
        name: A unique string name for the process.
    """
    def wrapper(func):
        REGISTRY[name] = func

        return func

    return wrapper

if global_settings.DEBUG:
    @register_process('wps.demo')
    @shared_task
    def demo(variables, operations, domains):
        logger.info('Operations {}'.format(operations))

        logger.info('Domains {}'.format(domains))

        logger.info('Variables {}'.format(variables))

        return cwt.Variable('file:///demo.nc', 'tas').parameterize()

def int_or_float(value):
    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        return None

class Status(object):
    """ Job Status interface.

    This interface wraps a Job model instance to provide access to updating
    its current message and percent completed.
    
    Attributes:
        job: A Job model instance.
        message: A string message.
        percent: A float value of the percent completed.
    """
    def __init__(self, job):
        self.job = job
        self.message = None
        self.percent = 0

    @classmethod
    def from_job_id(cls, job_id):
        """ Wraps a Job object.
        
        Args:
            job_id: The int id of the Job to wrap.

        Returns:
            An instance of Status.
        """
        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            job = None

        return cls(job)

    def update(self, message=None, percent=None):
        """ Updates a Jobs status.

        Update a jobs status. If message or percent are None then the last
        value of each will be used.

        Args:
            message: A string message.
            percent: A float value of the percent completed.
        """
        if message is not None:
            self.message = message

        if percent is not None:
            self.percent = percent

        logger.info('Update status {} {} %'.format(self.message, self.percent))

        if self.job is not None:
            self.job.update_progress(self.message, self.percent)

class CWTBaseTask(celery.Task):
    """ Compute Working Team (CWT) base celery task.

    This class provides common functionality for all CWT WPS Processes.

    *Note*
    This class is only instantiated once thus any state is shared between 
    all celery task decorators it is passed to.
    """

    def initialize(self, credentials=False, **kwargs):
        """ Initialize task.

        **kwargs has the following known values:
            job_id: An integer key of the current job.
            user_id: An integer key of the jobs user.
            cwd: A string path to change the current working directory to.
        
        Args:
            credentials: A boolean requesting credentials for the current task.
            **kwargs: A dict containing additional arguments.
        """
        if credentials:
            self.set_user_creds(**kwargs)

        return Status.from_job_id(kwargs.get('job_id'))

    def set_user_creds(self, **kwargs):
        """ Set the user credentials.

        Switches the current working directory then writes the user credentials
        in the current directory. A .dodsrc file is created to provide 
        credential access to underlying netCDF libraries.

        Args:
            **kwargs: A dict of options.
        """
        cwd = kwargs.get('cwd')

        # Write the user credentials
        user_id = kwargs.get('user_id')

        user_path = os.path.join(cwd, str(user_id))

        if not os.path.exists(user_path):
            os.mkdir(user_path)

        # Change the process working directory
        os.chdir(user_path)

        logger.info('Changed working directory to {}'.format(user_path))

        cred_path = os.path.join(user_path, 'creds.pem')

        try:
            user = models.User.objects.get(pk=user_id)
        except models.User.DoesNotExist:
            raise Exception('User {} does not exist'.format(user_id))

        with open(cred_path, 'w') as f:
            f.write(user.auth.cert)

        logger.info('Updated user credentials')

        # Clean up the old cookie file incase the previous attempt failed
        dods_cookies_path = os.path.join(user_path, '.dods_cookies')

        if os.path.exists(dods_cookies_path):
            logger.info('Cleared old cookies file.')

            os.remove(dods_cookies_path)

        # Write the dodsrc file if does not exist
        dodsrc_path = os.path.join(user_path, '.dodsrc')

        if not os.path.exists(dodsrc_path):
            with open(dodsrc_path, 'w') as f:
                f.write('HTTP.COOKIEJAR=.dods_cookies\n')
                f.write('HTTP.SSL.CERTIFICATE={}\n'.format(cred_path))
                f.write('HTTP.SSL.KEY={}\n'.format(cred_path))
                f.write('HTTP.SSL.CAPATH={}\n'.format(settings.CA_PATH))
                f.write('HTTP.SSL.VERIFY=0\n')

            logger.info('Wrote .dodsrc file')

    def load(self, variables, domains, operations):
        """ Load a processes inputs.

        Loads each value into their associated container class.

        Args:
            variables: A dict mapping names of Variables to their representations.
            domains: A dict mapping names of Domains to their representations.
            operations: A dict mapping names of Processes to their representations.

        Returns:
            A tuple of 3 dictionaries. Each dictionary maps unqiue names to an
            object of their respective container type.
        """
        v = dict((x, cwt.Variable.from_dict(y)) for x, y in variables.iteritems())

        d = dict((x, cwt.Domain.from_dict(y)) for x, y in domains.iteritems())

        for var in v.values():
            var.resolve_domains(d)

        o = dict((x, cwt.Process.from_dict(y)) for x, y in operations.iteritems())

        for op in o.values():
            op.resolve_inputs(v, o)

        if op.domain is not None:
            op.domain = d[op.domain]

        return v, d, o

    def op_by_id(self, name, operations):
        """ Retrieve an operation. """
        try:
            return [x for x in operations.values() if x.identifier == name][0]
        except IndexError:
            raise Exception('Could not find operation {}'.format(name))

    def generate_local_output(self, name=None):
        """ Format the file path for a local output. """
        if name is None:
            name = '{}.nc'.format(uuid.uuid4())

        path = os.path.join(settings.OUTPUT_LOCAL_PATH, name)

        return path

    def generate_output(self, local_path, **kwargs):
        """ Format the file path for a remote output. """
        if kwargs.get('local') is None:
            out_name = local_path.split('/')[-1]

            if settings.DAP:
                output = settings.DAP_URL.format(file_name=out_name)
            else:
                output = settings.OUTPUT_URL.format(file_name=out_name)
        else:
            output = 'file://{}'.format(local_path)

        return output

    def slice_to_str(self, s):
        """ Format a slice. """
        return '{}:{}:{}'.format(s.start, s.stop, s.step)

    def cache_multiple_input(self, input_vars, domain, read_callback=None):
        """ Cache multiple inputs.

        Map a domain over multiple inputs. The subset of each input is then 
        chunked and read_callback is called on each chunk. The subset of each 
        input will be inserted into the cache.

        Args:
            input_vars: A list of Variable objects.
            domain: A Domain object.
            read_callback: A method which takes a single argument which is a 
                numpy array.
        """
        var_name = reduce(lambda x, y: x if x == y else None, [x.var_name for x in input_vars])

        if var_name is None:
            raise Exception('Variable name is not the same for all inputs')

        logger.info('Aggregating {} files for variable {}'.format(len(input_vars), var_name))

        inputs = []

        try:
            for input_var in input_vars:
                inputs.append(cdms2.open(input_var.uri))
        except Exception:
            for i in inputs:
                i.close()

            raise AccessError()

        inputs = sorted(inputs, key=lambda x: x[var_name].getTime().units)

        logger.info('Sorted inputs by units')

        input_files = collections.OrderedDict([(x.id, x) for x in inputs])

        domain_map = self.map_domain_multiple(input_files.values(), var_name, domain)

        cache_map = {}

        # Check if each inputs subset has been cached.
        for input_url in domain_map.keys():
            temporal, spatial = domain_map[input_url]

            if temporal.stop == 0:
                logger.info('Skipping {}, not included in domain'.format(input_url))

                del input_files[input_url]

                continue

            cache, exists = self.check_cache(input_url, var_name, temporal, spatial)

            if exists:
                logger.info('Swapping {} for cached {}'.format(input_files[input_url].id, cache.id))

                input_files[input_url].close()

                input_files[input_url] = cache

                domain_map[input_url] = (slice(0, len(cache[var_name]), 1), {})
            else:
                logger.info('Caching {}'.format(input_url))

                cache_map[input_url] = cache

        if len(cache_map) > 0 or read_callback is not None:
            try:
                for input_url, input_file in input_files.iteritems():
                    logger.info('Processing input {}'.format(input_file.id))

                    temporal, spatial = domain_map[input_url]

                    tstart, tstop, tstep = temporal.start, temporal.stop, temporal.step

                    diff = tstop - tstart

                    step = diff if diff < 200 else 200

                    # Use a context on the input
                    with input_file as input_file:
                        for begin in xrange(tstart, tstop, step):
                            end = begin + step

                            if end > tstop:
                                end = tstop

                            time_slice = slice(begin, end, tstep)

                            logger.info('Retrieving chunk {}'.format(time_slice))

                            data = input_file(var_name, time=time_slice, **spatial)

                            if any(x == 0 for x in data.shape):
                                for cache in cache_map.values():
                                    cache.close()

                                raise InvalidShapeError('Data has shape {}'.format(data.shape))

                            if input_url in cache_map:
                                logger.info('Caching chunk {}'.format(data.shape))

                                cache_map[input_url].write(data, id=var_name)

                            if read_callback is not None:
                                logger.info('Post processing chunk {}'.format(data.shape))

                                read_callback(data)
            except InvalidShapeError:
                raise
            except Exception:
                for v in input_files.values():
                    v.close()

                raise AccessError()

            logger.info('Closing cache files {}'.format(cache_map.keys()))

            for cache in cache_map.values():
                cache.close()

    def cache_input(self, input_var, domain, read_callback=None):
        """ Cache input.

        Map a domain over an input. The subset of the input is then chunked
        and read_callback is called  on each chunk. The subset of input will
        be inserted into the cache.

        Args:
            input_vars: A list of Variable objects.
            domain: A Domain object.
            read_callback: A method which takes a single argument which is a 
                numpy array.
        """
        uri = input_var.uri

        var_name = input_var.var_name

        try:
            input_file = cdms2.open(input_var.uri)
        except:
            raise AccessError()

        temporal, spatial = self.map_domain(input_file, var_name, domain)

        cache, exists = self.check_cache(input_file.id, var_name, temporal, spatial) 

        if not exists:
            tstart, tstop, tstep = temporal.start, temporal.stop, temporal.step
        elif read_callback is not None:
            input_file.close()

            input_file = cache

            tstart, tstop, tstep = 0, len(input_file[var_name]), 1

            spatial = {}
        else:
            return cache

        diff = tstop - tstart

        step = diff if diff < 200 else 200

        try:
            with input_file as input_file, cache as cache:
                for begin in xrange(tstart, tstop, step):
                    end = begin + step

                    if end > tstop:
                        end = tstop

                    time_slice = slice(begin, end, tstep)

                    data = input_file(var_name, time=time_slice, **spatial)

                    if any(x == 0 for x in data.shape):
                        cache.close()

                        raise InvalidShapeError('Read data with shape {}'.format(data.shape))

                    if not exists:
                        cache.write(data, id=var_name)

                    if read_callback is not None:
                        read_callback(data)
        except InvalidShapeError:
            raise
        except Exception:
            raise AccessError()

    def generate_cache_name(self, uri, temporal, spatial):
        """ Create a cacheaable name.

        Args:
            uri: A string file uri.
            temporal: A slice over the temporal axis.
            spatial: A dict mapping axis names to slices.

        Returns:
            A unique cacheable name.
        """
        m = hashlib.sha256()

        m.update(uri)

        m.update(self.slice_to_str(temporal))

        for k in ['latitude', 'lat', 'y']:
            if k in spatial:
                m.update(self.slice_to_str(spatial[k]))

        for k in ['longitude', 'lon', 'x']:
            if k in spatial:
                m.update(self.slice_to_str(spatial[k]))

        return m.hexdigest()

    def check_cache(self, uri, var_name, temporal, spatial):
        """ Check cache for a file.

        Create a unique identifier using the URI of the input and the domain
        of the input it covers. If the file exists in the cache we validate
        it by checking the variable name and its shape. If the cache file is
        invalid, it is removed and a new one is created.

        Args:
            uri: A string path to the input.
            var_name: A string variable name in the input.
            temporal: A slice for the temporal axis.
            spatial: A dict of slices for the spatial axis.

        Returns:
            A FileVariable from CDMS2.
        """
        logger.info('Checking cache for file {} with domain {} {}'.format(uri, temporal, spatial))

        file_name = self.generate_cache_name(uri, temporal, spatial)

        file_name = '{}.nc'.format(file_name)

        file_path = '{}/{}'.format(settings.CACHE_PATH, file_name)

        exists = False

        if os.path.exists(file_path):
            logger.info('{} exists in the cache'.format(file_path))

            try:
                cache = cdms2.open(file_path)
            except cdms2.CDMSError:
                logger.info('Failed to open cache file')
            else:
                logger.info('Validating cache file')

                # Check for a valid cache file that is missing the variable
                if var_name in cache.variables:
                    time = cache[var_name].getTime().shape[0]

                    diff = temporal.stop - temporal.start

                    if diff < time or diff > time:
                        logger.info('Cache file is invalid expecting time {} got {}'.format(temporal.stop - temporal.start, time))

                        cache.close()

                        os.remove(file_path)
                    else:
                        logger.info('Cache exists and is valid')

                        exists = True
                else:
                    cache.close()

                    logger.info('Cache invalid variable {} not found in files {}'.format(var_name, cache.variables))
            
        if not exists:
            logger.info('{} does not exist in the cache'.format(file_path))

            cache = cdms2.open(file_path, 'w')

        return cache, exists

    def map_axis(self, axis, dim, clamp_upper=True):
        """ Map a dimension to an axis.

        Attempts to map a Dimension to a CoordinateAxis.

        Args:
            axis: A CoordinateAxis object.
            dim: A Dimension object.
            clamp_upper: A boolean indicating whether to include the last value.

        Raises:
            Exception: Could not map the Dimension to CoordinateAxis.
        """
        if dim.crs == cwt.INDICES:
            n = len(axis)

            end = dim.end

            if dim.start < 0 or dim.start > n:
                raise Exception('Starting index {} for dimension {} is out of bounds'.format(dim.start, dim.name))

            if dim.end < 0 or dim.end > n:
                if clamp_upper:
                    raise Exception('Ending index {} for dimension {} is out of bounds'.format(dim.end, dim.name))
                else:
                    end = n

            if dim.start > dim.end:
                raise Exception('Start and end indexes are invalid for dimension'.format(dim.name))

            axis_slice = slice(dim.start, end, dim.step)
        elif dim.crs == cwt.VALUES:
            try:
                start, stop = axis.mapInterval((dim.start, dim.end), indicator='con')
            except Exception:
                axis_slice = None
            else:
                axis_slice = slice(start, stop, dim.step)
        elif dim.crs == cwt.CRS('timestamps'):
            try:
                start, stop = axis.mapInterval((str(dim.start), str(dim.end)))
            except Exception:
                axis_slice = None
            else:
                axis_slice = slice(start, stop, dim.step)
        else:
            raise Exception('Unknown CRS {}'.format(dim.crs))

        logger.info('Mapped dimension {} to slice {}'.format(dim, axis_slice))

        return axis_slice

    def map_domain_multiple(self, inputs, var_name, domain):
        """ Map a Domain over multiple inputs.

        Maps each Dimension of a Domain over multiple inputs.

        Args:
            inputs: A list of FileVariable objects.
            var_name: A string variable name.
            domain: A Domain object.

        Returns:
            A dict mapping the input URIs to their subsets. Each subset is a 
            tuple of a 2 values. The first is the slice over the temporal axis.
            The second is a dictionary mapping each spatial axis name to a
            slice.
        """
        domain_map = {}

        inputs = sorted(inputs, key=lambda x: x[var_name].getTime().units)

        base = inputs[0][var_name].getTime()

        for inp in inputs:
            temporal = None
            spatial = {}

            if domain is None:
                logger.info('No domain defined, grabbing entire time axis')

                temporal = slice(0, len(inp[var_name]), 1)
            else:
                for dim in domain.dimensions:
                    axis_idx = inp[var_name].getAxisIndex(dim.name)

                    if axis_idx == -1:
                        raise Exception('Axis {} does not exist'.format(dim.name))

                    axis = inp[var_name].getAxis(axis_idx)

                    if axis.isTime():
                        clone_axis = axis.clone()

                        clone_axis.toRelativeTime(base.units)

                        temporal = self.map_axis(clone_axis, dim, clamp_upper=False)
                        
                        if dim.crs == cwt.INDICES:
                            dim.start -= temporal.start

                            dim.end -= (temporal.stop - temporal.start) + temporal.start
                    else:
                        spatial[dim.name] = self.map_axis(axis, dim)

            domain_map[inp.id] = (temporal, spatial)

            logger.info('Mapped domain {} to {} {}'.format(domain, inp.id, domain_map[inp.id]))

        return domain_map

    def map_domain(self, var, var_name, domain):
        """ Map a domain over an input.

        Map each Dimension of a Domain to the axis in the FileVariable.

        Args:
            var: A FileVariable object.
            var_name: A string variable name.
            domain: A Domain object.

        Returns:
            A tuple of 2 values. The first is a slice over the temporal axis.
            The seconda is a dictionary mapping spatial axis names to their
            slices.
        """
        temporal = None
        spatial = {}

        if domain is None:
            logger.info('No domain defined, grabbing entire time axis') 

            return slice(0, len(var[var_name]), 1), spatial

        for dim in domain.dimensions:
            axis_idx = var[var_name].getAxisIndex(dim.name)

            if axis_idx == -1:
                raise Exception('Axis {} does not exist'.format(dim.name))

            axis = var[var_name].getAxis(axis_idx)
            
            if axis.isTime():
                temporal = self.map_axis(axis, dim)
            else:
                spatial[dim.name] = self.map_axis(axis, dim)

        logger.info('Mapped domain {} to {} {}'.format(domain.name, var.id, (temporal, spatial)))
        
        return temporal, spatial

    def generate_grid(self, operation, variables, domains):
        """ Generate a grid.

        Args:
            operation: A Process object.
            variables: A dict mapping variable names to their respective object.
            domains: A dict mapping domain names to their respective objects.
        
        Returns:
            A tuple of 3 values. A TransientVariable containing the grid,
            a string of the tool, and a string of the method.
        """
        gridder = operation.parameters.get('gridder')

        grid = None
        tool = None
        method = None

        if gridder is not None:
            tool = gridder.tool

            method = gridder.method

            logger.info('Using {} of regridder {}'.format(method, tool))

            if isinstance(gridder.grid, (str, unicode)):
                if gridder.grid in variables:
                    v = variables[gridder.grid]

                    logger.info('Loading grid from {}'.format(v.uri))

                    self.grid_file = cdms2.open(v.uri)

                    grid = self.grid_file[v.var_name].getGrid()
                elif gridder.grid in domains:
                    d = domains[gridder.grid]
                    
                    lat = d.get_dimension(('latitude', 'lat', 'y'))

                    lon = d.get_dimension(('longitude', 'lon', 'x'))

                    lat_val = [int_or_float(x) for x in [lat.start, lat.end, lat.step]]

                    lon_val = [int_or_float(x) for x in [lon.start, lon.end, lon.step]]
                   
                    grid = cdms2.createUniformGrid(
                                                   lat_val[0], lat_val[1]-lat_val[0], lat_val[2],
                                                   lon_val[0], lon_val[1]-lon_val[0], lon_val[2])

                    logger.info('Created uniform grid for {}'.format(d))
                else:
                    grid_type, arg = gridder.grid.split('~')

                    if grid_type == 'gaussian':
                        grid = cdms2.createGaussianGrid(int(arg))

                        logger.info('Created gaussian grid {}'.format(arg))
                    elif grid_type == 'uniform':
                        lat_step, lon_step = arg.split('x')

                        lat_step = int_or_float(lat_step)

                        lon_step = int_or_float(lon_step)

                        grid = cdms2.createUniformGrid(90.0, 180/lat_step, -lat_step, 0.0, 360/lon_step, lon_step)

                        logger.info('Created uniform grid {}x{}'.format(lat_step, lon_step))

        return grid, str(tool), str(method)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """ Handle a failure. """
        try:
            job = models.Job.objects.get(pk=kwargs['job_id'])
        except KeyError:
            raise Exception('Job id was not passed to the task')
        except models.Job.DoesNotExist:
            raise Exception('Job {} does not exist'.format(kwargs['job_id']))

        job.failed(str(exc))

# Define after CWTBaseTask is declared
cwt_shared_task = partial(shared_task,
                          bind=True,
                          base=CWTBaseTask,
                          autoretry_for=(AccessError,),
                          retry_kwargs={'max_retries': 5})

@shared_task(bind=True, base=CWTBaseTask)
def handle_output(self, variable, **kwargs):
    self.initialize(**kwargs)

    job_id = kwargs.get('job_id')

    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        raise Exception('Job does not exist {}'.format(job_id))

    job.succeeded(json.dumps(variable))
