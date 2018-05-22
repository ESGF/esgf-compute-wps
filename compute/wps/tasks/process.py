#! /usr/bin/env python

import cdms2
import contextlib
import cwt
import datetime
import math
import os
import re
from cdms2 import MV2 as MV
from celery.utils.log import get_task_logger

from wps import models
from wps import settings
from wps import WPSError
from wps.tasks import base
from wps.tasks import credentials
from wps.tasks import file_manager

logger = get_task_logger('wps.tasks.process')

class Process(object):
    def __init__(self, task_id):
        self.task_id = task_id

        self.process = None

        self.user = None

        self.job = None

    def initialize(self, user_id, job_id, process_id=None):
        try:
            self.user = models.User.objects.get(pk=user_id)
        except models.User.DoesNotExist:
            raise WPSError('User with id "{id}" does not exist', id=user_id)

        try:
            self.job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise WPSError('Job with id "{id}" does not exist', id=job_id)

        if process_id is not None:
            try:
                self.process = models.Process.objects.get(pk=process_id)
            except models.Process.DoesNotExist:
                raise WPSError('Process with id "{id}" does not exist', id=process_id)

        credentials.load_certificate(self.user)

    def log(self, fmt, *args, **kwargs):
        data = fmt.format(*args)

        msg = '[{}] {}'.format(self.task_id, data)

        self.job.update_status(msg, **kwargs)

        logger.info(msg)

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

    def generate_grid(self, gridder, spatial, chunk):
        try:
            grid_type, grid_param = gridder.grid.split('~')
        except ValueError:
            raise WPSError('Error generating grid "{name}"', name=gridder.grid)

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
        else:
            try:
                nlats = int(grid_param)
            except ValueError:
                raise WPSError('Error converting gaussian parameter to an int')

            grid = cdms2.createGaussianGrid(nlats)

            logger.info('Created target gaussian grid {}'.format(grid.shape))

        target = cdms2.MV2.ones(grid.shape)

        target.setAxisList(grid.getAxisList())

        lat = chunk.getLatitude()

        lon = chunk.getLongitude()

        try:
            lat_spec = spatial[lat.id]

            lon_spec = spatial[lon.id]
        except KeyError as e:
            logger.debug('Skipping subsetting the target grid')
        else:
            target = target(latitude=lat_spec, longitude=lon_spec)

        return target.getGrid()

    def check_cache(self, operation):
        base_units = None

        with file_manager.DataSetCollection.from_variables(operation.inputs) as collection:
            for dataset in collection.datasets:
                try:
                    dataset.map_domain(operation.domain, collection.get_base_units())
                except file_manager.DomainMappingError:
                    continue

                domain = collection.generate_dataset_domain(dataset)

                if collection.get_cache_entry(dataset, domain) is None:
                    return False

        return True

    def generate_chunk_map(self, operation):
        chunk_map = {}

        collections = [
            file_manager.DataSetCollection.from_variables(operation.inputs)
        ]
        
        fm = file_manager.FileManager(collections)

        for collection in fm.collections:
            with collection as collection:
                for dataset, chunk in collection.partitions(operation.domain, True, skip_data=True):
                    if dataset.url in chunk_map:
                        chunk_map[dataset.url]['chunks'].append(chunk)
                    else:
                        chunk_map[dataset.url] = {
                            'variable_name': fm.get_variable_name(),
                            'base_units': collection.get_base_units(),
                            'temporal': dataset.temporal,
                            'spatial': dataset.spatial,
                            'chunks': [chunk,]
                        }

        return chunk_map

    def retrieve_data(self, operation, num_inputs, output_file):
        grid = None
        gridder = operation.get_parameter('gridder')

        start = datetime.datetime.now()

        collections = [
            file_manager.DataSetCollection.from_variables(operation.inputs)
        ]

        with file_manager.FileManager(collections) as fm:
            var_name = fm.get_variable_name() 

            self.log('Retrieving variable "{}"', var_name)

            with fm.collections[0] as collection:
                base_units = collection.get_base_units()

                last_url = None

                start = datetime.datetime.now()

                for meta in collection.partitions(operation.domain, False):
                    ds, chunk = meta

                    if last_url != ds.url:
                        self.log('Starting to retrieve "{}"'.format(ds.file_obj.id))

                        last_url = ds.url

                    self.log('Retrieved chunk shape {}'.format(chunk.shape))

                    chunk.getTime().toRelativeTime(base_units)

                    if gridder is not None:
                        if grid is None:
                            grid = self.generate_grid(gridder, ds.spatial, chunk)

                        chunk = chunk.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

                    output_file.write(chunk, id=var_name)

                stat = os.stat(output_file.id)

                delta = datetime.datetime.now() - start

                self.process.update_rate(stat.st_size/1048576.0, delta.seconds)

        stop = datetime.datetime.now()

        final_shape = output_file[var_name].shape

        self.log('Finish retrieving all files, final shape "{}", elapsed time {}', final_shape, stop-start, percent=100)

        return var_name

    def process_data(self, operation, num_inputs, output_file, process):
        grid = None

        gridder = operation.get_parameter('gridder')

        start = datetime.datetime.now()

        axes = operation.get_parameter('axes', True)

        axes = axes.values[0]

        self.log('Starting to process inputs')

        result_list = []

        if len(operation.inputs) == 1 or num_inputs == 1:
            collections = [
                file_manager.DataSetCollection.from_variables(operation.inputs)
            ]
        else:
            collections = [
                file_manager.DataSetCollection.from_variables([x]) 
                for x in operation.inputs
            ]

        with file_manager.FileManager(collections) as fm:
            output_list = []

            var_name = fm.get_variable_name()

            with contextlib.nested(*[x for x in fm.collections]):
                over_temporal = fm.collections[0].datasets[0].get_time().id == axes

                start = datetime.datetime.now()

                for meta in fm.partitions(operation.domain, axes, num_inputs):
                    data_list = []
                    axis_index = None

                    for item in meta:
                        ds, chunk = item

                        if axis_index is None:
                            axis_index = ds.get_variable().getAxisIndex(axes)

                        if gridder is not None:
                            if grid is None:
                                grid = self.generate_grid(gridder, ds.spatial, chunk)

                            if not over_temporal:
                                chunk = chunk.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

                        data_list.append(chunk)

                    if len(data_list) == 0:
                        break

                    if len(data_list) > 1:
                        result_data = process(*data_list)
                    else:
                        result_data = process(*data_list, axis=axis_index)

                    self.log('Process output shape {}'.format(result_data.shape))

                    if over_temporal:
                        result_list.append(result_data)
                    else:
                        output_file.write(result_data, id=var_name)

                if over_temporal:
                    data = MV.concatenate(result_list)

                    if grid is not None:
                        data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

                    output_file.write(data, id=var_name)

                stat = os.stat(output_file.id)

                delta = datetime.datetime.now() - start

                self.process.update_rate(stat.st_size/1048576.0, delta.seconds)

        stop = datetime.datetime.now()

        final_shape = output_file[var_name].shape

        self.log('Finish retrieving all files, final shape "{}", elapsed time {}', final_shape, stop-start, percent=100)

        return var_name
