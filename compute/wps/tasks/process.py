#! /usr/bin/env python

import cdms2
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

    def retrieve(self, fm, operation, num_inputs, output_file):
        gridder = operation.get_parameter('gridder')

        if gridder is not None:
            grid = self.generate_grid(gridder)

        start = datetime.datetime.now()

        base_units = None

        matched = reduce(lambda x, y: x if x == y else None, fm.datasets)

        if matched is None:
            raise WPSError('Error variable name is not the same throughout all files')

        for percent, dataset in fm.sorted(num_inputs):
            if base_units is None:
                base_units = dataset.get_time().units

            with dataset:
                self.log('Retrieving input "{}" with shape "{}"', dataset.url, dataset.shape, percent=percent)

                self.log('Mapping domain to file', percent=percent)

                dataset.map_domain(operation.domain, base_units)

                logger.info(dataset.temporal)

                if dataset.temporal is None:
                    self.log('Skipping "{}"'.format(dataset.url), percent=percent)

                    continue

                self.log('Checking cache for file', percent=percent)

                dataset.check_cache()

                for data_percent, temporal, spatial in dataset.partitions('time'):
                    data = dataset.file_obj(dataset.variable_name, time=temporal, **spatial)

                    self.log('Retrieved slice {} with shape {} {}%', temporal, data.shape, data_percent, percent=percent)

                    if dataset.cache_obj is not None:
                        dataset.cache_obj.write(data, id=dataset.variable_name)

                        dataset.cache_obj.sync()

                    data.getTime().toRelativeTime(base_units)

                    if gridder is not None:
                        data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

                    output_file.write(data, id=matched.variable_name)

                self.log('Finished retrieving file "{}"', dataset.url, percent=percent)

        stop = datetime.datetime.now()

        final_shape = output_file[matched.variable_name].shape

        self.log('Finish retrieving all files, final shape "{}", elapsed time {}', final_shape, stop-start, percent=100)

        return matched.variable_name

    def process(self, fm, operation, num_inputs, output_file, process):
        gridder = operation.get_parameter('gridder')

        if gridder is not None:
            grid = self.generate_grid(gridder)

        start = datetime.datetime.now()

        matched = reduce(lambda x, y: x if x == y else None, fm.datasets)

        if matched is None:
            raise WPSError('Error variable name is not the same throughout all files')

        axes = operation.get_parameter('axes', True)

        if axes is None:
            raise base.WPSError('Missing required parameter axes')

        self.log('Checking cache for inputs files')

        over_temporal = fm.datasets[0].get_time().id == axes.values[0]

        if not over_temporal:
            for dataset in fm.datasets:
                dataset.check_cache()

        self.log('Starting to process inputs')

        result_list = []

        for partitions in fm.partitions(axes.values[0], num_inputs):
            data_list = []

            logger.info('Processing partitions "{}"'.format(partitions))

            for dataset, dataset_partition in zip(fm.datasets, partitions):
                _, temporal, spatial = dataset_partition

                self.log('Retrieving partition "{}" "{}" for dataset "{}"', temporal, spatial, dataset.url)

                axis_index = dataset.file_obj[dataset.variable_name].getAxisIndex(axes.values[0])

                logger.info('Processing over axis index {}'.format(axis_index))

                data = dataset.file_obj(dataset.variable_name, time=temporal, **spatial)

                if dataset.cache_obj is not None:
                    logger.info('Writing cache file "{}"'.format(dataset.cache_obj.id))

                    dataset.cache_obj.write(data, id=dataset.variable_name)

                    dataset.cache_obj.sync()

                if gridder is not None:
                    logger.info('Regrid before shape "{}"'.format(data.shape))

                    data = data.regrid(grid, regridTool=gridder.tool, regridMethod=gridder.method)

                    logger.info('Regrid aftter shape "{}"'.format(data.shape))

                data_list.append(data)

            result_data = process(*data_list, axis=axis_index)

            if over_temporal:
                result_list.append(result_data)
            else:
                output_file.write(result_data, id=matched.variable_name)

        if over_temporal:
            output_file.write(MV.concatenate(result_list), id=matched.variable_name)

        stop = datetime.datetime.now()

        final_shape = output_file[matched.variable_name].shape

        self.log('Finish retrieving all files, final shape "{}", elapsed time {}', final_shape, stop-start, percent=100)

        return matched.variable_name
