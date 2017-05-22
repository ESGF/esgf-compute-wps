#! /usr/bin/env python

import collections
import copy
import hashlib
import json
import os
import uuid
from contextlib import closing

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

__all__ = ['REGISTRY', 'register_process', 'get_process', 'CWTBaseTask', 'handle_output']

logger = get_task_logger('wps.processes.process')

REGISTRY = {}

def get_process(name):
    try:
        return REGISTRY[name]
    except KeyError:
        raise Exception('Process {} does not exist'.format(name))

def register_process(name):
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
    def __init__(self, job):
        self.job = job
        self.message = None
        self.percent = 0

    @classmethod
    def from_job_id(cls, job_id):
        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            job = None

        return cls(job)

    def update(self, message=None, percent=None):
        if message is not None:
            self.message = message

        if percent is not None:
            self.percent = percent

        logger.info('Update status {} {} %'.format(self.message, self.percent))

        if self.job is not None:
            self.job.update_progress(self.message, self.percent)

class CWTBaseTask(celery.Task):
    def initialize(self, credentials=False, **kwargs):
        task_id = self.request.id

        if credentials:
            self.set_user_creds(**kwargs)

        self.grid_file = None

        return Status.from_job_id(kwargs.get('job_id'))

    def cache_file(self, file_name, domain_map):
        m = hashlib.sha256()

        temporal, spatial = domain_map[file_name]

        time = ':'.join(str(x) for x in temporal)

        lat = None
        lon = None

        for lat_id in ['latitude', 'lat', 'y']:
            if lat_id in spatial:
                s = spatial[lat_id]

                if isinstance(s, slice):
                    lat = '{}:{}:{}'.format(s.start, s.stop, s.step)
                else:
                    lat = ':'.join(str(x) for x in s)

                break

        for lon_id in ['longitude', 'lon', 'x']:
            if lon_id in spatial:
                s = spatial[lon_id]

                if isinstance(s, slice):
                    lat = '{}:{}:{}'.format(s.start, s.stop, s.step)
                else:
                    lon = ':'.join(str(x) for x in s)

                break

        m.update('{}~{}~{}~{}'.format(file_name, time, lat, lon))

        file_name = '{}.nc'.format(m.hexdigest())

        file_path = '{}/{}'.format(settings.CACHE_PATH, file_name)

        if os.path.exists(file_path):
            return file_path, True

        return file_path, False

    def set_user_creds(self, **kwargs):
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

    def cleanup(self):
        if self.grid_file is not None:
            self.grid_file.close()

    def generate_grid(self, operation, variables, domains):
        gridder = operation.parameters.get('gridder')

        grid = None
        tool = None
        method = None

        if gridder is not None:
            tool = gridder.tool

            method = gridder.method

            if isinstance(gridder.grid, (str, unicode)):
                if gridder.grid in variables:
                    v = variables[gridder.grid]

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
                else:
                    grid_type, arg = gridder.grid.split('~')

                    if grid_type == 'gaussian':
                        grid = cdms2.createGaussianGrid(int(arg))
                    elif grid_type == 'uniform':
                        lat_step, lon_step = arg.split('x')

                        lat_step = int_or_float(lat_step)

                        lon_step = int_or_float(lon_step)

                        grid = cdms2.createUniformGrid(90.0, 180/lat_step, -lat_step, 0.0, 360/lon_step, lon_step)

        return grid, tool, method

    def build_domain(self, inputs, domains, var_name):
        domain_map = collections.OrderedDict()
        current = 0

        for idx, i in enumerate(inputs):
            temporal = (0, len(i[var_name]), 1)

            spatial = {}

            dimensions = None
                        
            if 'global' in domains:
                dimensions = domains.get('global').dimensions
            elif i.id in domains:
                dimensions = domains[i.id].dimensions

            if dimensions is not None:
                axes = dict((x.id, x) for x in i[var_name].getAxisList())

                for dim in dimensions:
                    if dim.name == 'time' or (dim.name in axes and axes[dim.name].isTime()):
                        if dim.crs == cwt.INDICES:
                            if dim.start > current:
                                start = dim.start
                            else:
                                start = 0

                            if (current + len(i[var_name])) > dim.end:
                                end = dim.end - current
                            else:
                                end = len(i[var_name])

                            temporal = (start, end, dim.step)
                        elif dim.crs == cwt.VALUES:
                            try:
                                start, stop = axes[dim.name].mapInterval((dim.start, dim.end), 'co')
                            except KeyError:
                                raise Exception('Dimension {} could not be mapped, does not exist'.format(dim.name))

                            dim.start -= start

                            if dim.start < 0:
                                dim.start = 0

                            dim.end -= stop

                            temporal = (start, stop, dim.step)
                        else:
                            raise Exception('Unknown CRS value {}'.format(dim.crs))
                    else:
                        if dim.crs == cwt.INDICES:
                            spatial[dim.name] = slice(dim.start, dim.end, dim.step)
                        elif dim.crs == cwt.VALUES:
                            try:
                                start, stop = axes[dim.name].mapInterval((dim.start, dim.end), 'co')
                            except KeyError:
                                raise Exception('Dimension {} could not be mapped, does not exist'.format(dim.name))

                            spatial[dim.name] = slice(start, stop, dim.step)
                        else:
                            raise Exception('Unknown CRS value {}'.format(dim.crs))

            domain_map[i.id] = (temporal, spatial)

            current += len(i[var_name])

        return domain_map

    def op_by_id(self, name, operations):
        try:
            return [x for x in operations.values() if x.identifier == name][0]
        except IndexError:
            raise Exception('Could not find operation {}'.format(name))

    def generate_local_output(self, name=None):
        if name is None:
            name = '{}.nc'.format(uuid.uuid4())

        path = os.path.join(settings.OUTPUT_LOCAL_PATH, name)

        return path

    def generate_output(self, local_path, **kwargs):
        if kwargs.get('local') is None:
            out_name = local_path.split('/')[-1]

            output = settings.OUTPUT_URL.format(file_name=out_name)
        else:
            output = 'file://{}'.format(local_path)

        return output

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        try:
            job = models.Job.objects.get(pk=kwargs['job_id'])
        except KeyError:
            raise Exception('Job id was not passed to the task')
        except models.Job.DoesNotExist:
            raise Exception('Job {} does not exist'.format(kwargs['job_id']))

        job.failed(str(exc))

@shared_task(bind=True, base=CWTBaseTask)
def handle_output(self, variable, **kwargs):
    self.initialize(**kwargs)

    job_id = kwargs.get('job_id')

    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        raise Exception('Job does not exist {}'.format(job_id))

    job.succeeded(json.dumps(variable))
