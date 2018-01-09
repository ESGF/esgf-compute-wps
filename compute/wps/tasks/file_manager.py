#! /usr/bin/env python

import cdms2
from celery.utils.log import get_task_logger

from wps.tasks import base

logger = get_task_logger('wps.tasks.file_manager')

__ALL__ = [
    'DataSet',
    'FileManager',
]

class DataSet(object):
    def __init__(self, file_obj, url, variable):
        self.file_obj = file_obj

        self.url = url

        self.variable = variable

        self.time = None

    def get_time(self):
        if self.time is None:
            try:
                self.time = self.file_obj[self.variable].getTime()
            except cdms2.CDMSError as e:
                raise base.AccessError(self.url, e.message)

        return self.time

    def close(self):
        if self.file_obj is not None:
            self.file_obj.close()

            self.file_obj = None

    def __del__(self):
        self.close()

class FileManager(object):
    def __init__(self, datasets):
        self.datasets = datasets

    @classmethod
    def from_cwt_variables(cls, variables, keep=None):
        datasets = []

        try:
            for var in variables:
                file_obj = cdms2.open(var.uri)

                datasets.append(DataSet(file_obj, var.uri, var.var_name))
        except cdms2.CDMSError as e:
            for ds in datasets:
                ds.close()

            raise base.AccessError(var.uri, e.message)

        datasets = sorted(datasets, key=lambda x: x.get_time().units)

        if keep is not None:
            for x in xrange(keep, len(datasets)):
                datasets[x].close()

            datasets = datasets[:keep]

        return cls(datasets)

    def close(self):
        for ds in self.datasets:
            ds.close()

    def __del__(self):
        self.close()
