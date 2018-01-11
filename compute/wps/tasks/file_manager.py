#! /usr/bin/env python

import cdms2
import cwt
from celery.utils.log import get_task_logger

from wps.tasks import base

__ALL__ = [
    'DataSet',
    'FileManager',
]

logger = get_task_logger('wps.tasks.file_manager')

class DataSet(object):
    def __init__(self, file_obj, url, variable_name):
        self.file_obj = file_obj

        self.url = url

        self.variable_name = variable_name

        self.time_axis = None

        self.temporal = None

        self.spatial = {}

    def str_to_int(self, value):
        try:
            return int(value)
        except ValueError:
            raise base.WPSError('Could not convert "{value}" to int', value=value)

    def str_to_int_float(self, value):
        try:
            return self.str_to_int(value)
        except base.WPSError:
            pass

        try:
            return float(value)
        except ValueError:
            raise base.WPSError('Could not convert "{value}" to float or int', value=value)

    def dimension_to_cdms2_selector(self, dimension):
        if dimension.crs == cwt.VALUES:
            start = self.str_to_int_float(dimension.start)

            end = self.str_to_int_float(dimension.end)

            selector = (start, end)
        elif dimension.crs == cwt.INDICES:
            start = self.str_to_int(dimension.start)

            end = self.str_to_int(dimension.end)

            step = self.str_to_int(dimension.step)

            selector = slice(start, end, step)
        else:
            raise base.WPSError('Error handling CRS "{name}"', name=dimension.crs)

        return selector

    def map_domain(self, domain):
        variable = self.file_obj[self.variable_name]

        for dim in domain.dimensions:
            axis_index = variable.getAxisIndex(dim.name)

            if axis_index == -1:
                raise base.WPSError('Dimension "{name}" was not found in "{url}"', name=dim.name, url=self.url)

            axis = variable.getAxis(axis_index)

            if axis.isTime():
                self.temporal = self.dimension_to_cdms2_selector(dim)
            else:
                self.spatial[dim.name] = self.dimension_to_cdms2_selector(dim)

    def get_time(self):
        if self.time_axis is None:
            try:
                self.time_axis = self.file_obj[self.variable_name].getTime()
            except cdms2.CDMSError as e:
                raise base.AccessError(self.url, e.message)

        return self.time_axis

    def close(self):
        if self.file_obj is not None:
            self.file_obj.close()

            self.file_obj = None

    def __del__(self):
        self.close()

    def __repr__(self):
        return 'DataSet(url={url}, variable_name={variable_name}, temporal_roi={temporal}, spatial_roi={spatial})'.format(
                url=self.url,
                variable_name=self.variable_name,
                temporal=self.temporal,
                spatial=self.spatial
            )

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
