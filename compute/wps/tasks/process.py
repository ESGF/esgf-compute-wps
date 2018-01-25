#! /usr/bin/env python

import cdms2
import contextlib
import cwt
import datetime
import math
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

        self.user = None

        self.job = None

    def initialize(self, user_id, job_id):
        try:
            self.user = models.User.objects.get(pk=user_id)
        except models.User.DoesNotExist:
            raise WPSError('User with id "{id}" does not exist', id=user_id)

        try:
            self.job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise WPSError('Job with id "{id}" does not exist', id=job_id)

        credentials.load_certificate(self.user)

    def log(self, fmt, *args, **kwargs):
        data = fmt.format(*args)

        msg = '[{}] {}'.format(self.task_id, data)

        self.job.update_status(msg, **kwargs)

        logger.info(msg)

    def generate_grid(self, gridder):
        try:
            grid_type, grid_param = gridder.grid.split('~')
        except ValueError:
            raise WPSError('Error generating grid "{name}"', name=gridder.grid)

        if grid_type.lower() == 'uniform':
            try:
                nlats, nlons = grid_param.split('x')

                nlats = int(nlats)

                nlons = int(nlons)
            except ValueError:
                raise WPSError('Error parsing parameters "{value}" for uniform grid', value=grid_param)

            grid = cdms2.createUniformGrid(0, nlats, 1, 0, nlons, 1)
        else:
            try:
                nlats = int(grid_param)
            except ValueError:
                raise WPSError('Error converting gaussian parameter to an int')

            grid = cdms2.createGaussianGrid(nlats)

        return grid

    def retrieve(self, operation, num_inputs, output_file):
        gridder = operation.get_parameter('gridder')

        if gridder is not None:
            grid = self.generate_grid(gridder)

            logger.info('Regridding to new grid "{}"'.format(grid.shape))

        start = datetime.datetime.now()

        with file_manager.FileManager(operation.inputs) as fm:
            var_name = fm.get_variable_name() 

            self.log('Retrieving variable "{}"', var_name)

            with fm.collections[0] as collection:
                base_units = collection.get_base_units()

                last_url = None

                for meta in collection.partitions(operation.domain, False):
                    ds, chunk = meta

                    if last_url != ds.url:
                        self.log('Starting to retrieve "{}"'.format(ds.url))

                        last_url = ds.url

                    self.log('Retrieved chunk shape {}'.format(chunk.shape))

                    chunk.getTime().toRelativeTime(base_units)

                    if gridder is not None:
                        chunk = chunk.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

                    output_file.write(chunk, id=var_name)


        stop = datetime.datetime.now()

        final_shape = output_file[var_name].shape

        self.log('Finish retrieving all files, final shape "{}", elapsed time {}', final_shape, stop-start, percent=100)

        return var_name

    def process(self, operation, num_inputs, output_file, process):
        gridder = operation.get_parameter('gridder')

        if gridder is not None:
            grid = self.generate_grid(gridder)

        start = datetime.datetime.now()

        axes = operation.get_parameter('axes', True)

        if axes is None:
            raise base.WPSError('Missing required parameter axes')

        self.log('Starting to process inputs')

        result_list = []

        with file_manager.FileManager(operation.inputs) as fm:
            output_list = []

            var_name = fm.get_variable_name()

            with contextlib.nested(*[x for x in fm.collections]):
                over_temporal = fm.collections[0].datasets[0].get_time().id == axes.values[0]

                for meta in fm.partitions(operation.domain, axes.values[0], num_inputs):
                    data_list = []
                    axis_index = None

                    for item in meta:
                        ds, chunk = item

                        if axis_index is None:
                            axis_index = ds.get_variable().getAxisIndex(axes.values[0])

                        if gridder is not None:
                            chunk = chunk.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

                        data_list.append(chunk)

                    if len(data_list) == 0:
                        break

                    result_data = process(*data_list, axis=axis_index)

                    self.log('Process output shape {}'.format(result_data.shape))

                    if over_temporal:
                        result_list.append(result_data)
                    else:
                        output_file.write(result_data, id=var_name)

                if over_temporal:
                    output_file.write(MV.concatenate(result_list), id=var_name)

        stop = datetime.datetime.now()

        final_shape = output_file[var_name].shape

        self.log('Finish retrieving all files, final shape "{}", elapsed time {}', final_shape, stop-start, percent=100)

        return var_name
