#! /usr/bin/env python

import cdms2
import cwt
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

    def retrieve(self, fm, operation, output_path):
        self.job.started()

        try:
            output_file = cdms2.open(output_path, 'w')
        except cdms2.CDMSError as e:
            raise base.AccessError(output_path, e.message)

        logger.info('Writing output to "{}"'.format(output_path))

        with output_file:
            for dataset in fm.datasets:
                self.log('Retrieving input "{}" with shape "{}"', dataset.url, dataset.shape)

                if operation.domain is not None:
                    self.log('Mapping domain to file')

                    dataset.map_domain(operation.domain)

                self.log('Checking cache for file')

                dataset.check_cache()

                for temporal, spatial in dataset.partitions('time'):
                    data = dataset.file_obj(dataset.variable_name, time=temporal, **spatial)

                    self.log('Retrieved slice {} with shape {}', temporal, data.shape)

                    if dataset.cache_obj is not None:
                        dataset.cache_obj.write(data, id=dataset.variable_name)

                    output_file.write(data, id=dataset.variable_name)

                self.log('Finished retrieving file "{}"', dataset.url)

            self.log('Finish retrieving all files, final shape "{}"', output_file[dataset.variable_name].shape)

        return output_path

    def process(self, fm, operation):
        return cwt.Variable('file:///test.nc', 'tas')
