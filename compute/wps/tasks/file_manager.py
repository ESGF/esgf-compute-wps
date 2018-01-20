#! /usr/bin/env python

import cdms2
import cwt
import hashlib
import json
import math
from celery.utils.log import get_task_logger

from wps import models
from wps import settings
from wps.tasks import base

__ALL__ = [
    'DataSet',
    'FileManager',
]

logger = get_task_logger('wps.tasks.file_manager')

class DataSet(object):
    def __init__(self, file_obj, url, variable_name):
        self.file_obj = file_obj

        self.cache_obj = None

        self.cache = None

        self.url = url

        self.variable_name = variable_name

        self.temporal_axis = None

        self.spatial_axis = {}

        self.temporal = None

        self.spatial = {}

    @property
    def shape(self):
        if self.file_obj is not None:
            return self.file_obj[self.variable_name].shape

        return None

    def partitions(self, axis_name):
        axis = None

        if (self.temporal_axis is not None and 
                self.temporal_axis.id == axis_name):
            axis = self.temporal_axis
        elif (axis_name in self.spatial_axis):
            axis = self.spatial_axis[axis_name]
        else:
            axis_index = self.file_obj[self.variable_name].getAxisIndex(axis_name)

            if axis_index == -1:
                raise base.WPSError('Could not find axis "{name}"', name=axis_name)

            axis = self.file_obj[self.variable_name].getAxis(axis_index)

        if axis.isTime():
            if self.temporal is None:
                start = 0

                stop = axis.shape[0]
            elif isinstance(self.temporal, slice):
                start = self.temporal.start

                stop = self.temporal.stop

            diff = stop - start

            step = min(diff, settings.PARTITION_SIZE)

            n = math.ceil((stop-start)/step) + 1

            for i, begin in enumerate(xrange(start, stop, step)):
                end = min(begin + step, stop)

                yield round((i+1.0)*100.0/n, 2), slice(begin, end), self.spatial
        else:
            raise base.WPSError('Partitioning along a spatial axis is unsupported')

    def check_cache(self):
        uid = '{}:{}'.format(self.url, self.variable_name)

        uid_hash = hashlib.sha256(uid).hexdigest()

        domain = {
            'variable': self.variable_name,
            'temporal': None,
            'spatial': {}
        }

        if isinstance(self.temporal, (list, tuple)):
            indices = self.get_time().mapInterval(self.temporal)

            domain['temporal'] = slice(indices[0], indices[1])
        else:
            domain['temporal'] = self.temporal

        for name in self.spatial.keys():
            if isinstance(self.spatial[name], (list, tuple)):
                indices = self.get_axis(name).mapInterval(self.spatial[name])

                domain['spatial'][name] = slice(indices[0], indices[1])
            else:
                domain['spatial'][name] = self.spatial[name]

        logger.info('Checking cache for "{}" with domain "{}"'.format(uid_hash, domain))

        cache_entries = models.Cache.objects.filter(uid=uid_hash)

        logger.info('Found "{}" cache entries matching hash "{}"'.format(len(cache_entries), uid_hash))

        for x in xrange(cache_entries.count()):
            cache = cache_entries[x]

            logger.info('Checking cache entry with dimensions "{}"'.format(cache.dimensions))
            
            if not cache.valid:
                logger.info('Cache entry hash "{}" with dimensions "{}" invalid'.format(cache.uid, cache.dimensions))

                cache.delete()
                
                continue

            if cache.is_superset(domain):
                logger.info('Cache entry is a superset of the requested domain')

                self.cache = cache

                break

        if self.cache is None:
            logger.info('Creating cache file for "{}"'.format(self.url))

            dimensions = json.dumps(domain, default=models.slice_default)

            self.cache = models.Cache.objects.create(uid=uid_hash, url=self.url, dimensions=dimensions)

            try:
                self.cache_obj = cdms2.open(self.cache.local_path, 'w')
            except cdms2.CDMSError as e:
                logger.exception('Error creating cache file "{}": {}'.format(self.cache.local_path, e.message))

                self.cache.delete()

                self.cache = None

                pass
        else:
            logger.info('Using cache file "{}" as input'.format(self.cache.local_path))

            try:
                cache_obj = cdms2.open(self.cache.local_path)
            except cdms2.CDMSError as e:
                logger.exception('Error opening cached file "{}": {}'.format(self.cache.local_path, e.message))

                self.cache.delete()

                self.cache = None
                
                pass
            else:
                self.close()

                self.file_obj = cache_obj

                logger.info('Swapped source for cached file')

    def dimension_to_cdms2_selector(self, dimension, axis, base_units=None):
        if dimension.crs == cwt.VALUES:
            start = dimension.start

            end = dimension.end

            if axis.isTime():
                axis_clone = axis.clone()

                axis_clone.toRelativeTime(base_units)

                try:
                    start, end = axis_clone.mapInterval((start, end))
                except TypeError:
                    logger.exception('Error mapping dimension "{}"'.format(dimension.name))

                    selector = None

                    pass
                else:
                    selector = slice(start, end)
            else:
                selector = (start, end)
        elif dimension.crs == cwt.INDICES:
            n = axis.shape[0]

            selector = slice(dimension.start, min(dimension.end, n), dimension.step)

            dimension.start -= selector.start

            dimension.end -= (selector.stop - selector.start) + selector.start
        else:
            raise base.WPSError('Error handling CRS "{name}"', name=dimension.crs)

        return selector

    def map_domain(self, domain, base_units):
        logger.info('Mapping domain "{}"'.format(domain))

        variable = self.file_obj[self.variable_name]

        if domain is None:
            if variables.getTime() == None:
                self.temporal = None
            else:
                self.temporal = slice(0, variable.getTime().shape[0])

            axes = variable.getAxisList()

            for axis in axes:
                if axis.isTime():
                    continue

                self.spatial[axis.id] = slice(0, axis.shape[0])
        else:
            for dim in domain.dimensions:
                axis_index = variable.getAxisIndex(dim.name)

                if axis_index == -1:
                    raise base.WPSError('Dimension "{name}" was not found in "{url}"', name=dim.name, url=self.url)

                axis = variable.getAxis(axis_index)

                if axis.isTime():
                    self.temporal = self.dimension_to_cdms2_selector(dim, axis, base_units)

                    logger.info('Mapped temporal domain to "{}"'.format(self.temporal))
                else:
                    self.spatial[dim.name] = self.dimension_to_cdms2_selector(dim, axis)

                    logger.info('Mapped spatial "{}" to "{}"'.format(dim.name, self.spatial[dim.name]))

    def get_time(self):
        if self.temporal_axis is None:
            try:
                self.temporal_axis = self.file_obj[self.variable_name].getTime()
            except cdms2.CDMSError as e:
                raise base.AccessError(self.url, e.message)

        return self.temporal_axis

    def get_axis(self, name):
        if name not in self.spatial_axis:
            axis_index = self.file_obj[self.variable_name].getAxisIndex(name)

            if axis_index == -1:
                raise base.WPSError('Axis "{name}" does not exist', name=name)

            self.spatial_axis[name] = self.file_obj[self.variable_name].getAxis(axis_index)

        return self.spatial_axis[name]

    def close(self):
        if self.temporal_axis is not None:
            self.temporal_axis = None

        self.spatial_axis = {}
        
        if self.file_obj is not None:
            self.file_obj.close()

            self.file_obj = None

    def __eq__(self, other):
        if not isinstance(other, DataSet):
            return False

        return self.variable_name == other.variable_name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

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
    def __init__(self, variables):
        self.variables = variables

        self.datasets = []

    def __enter__(self):
        try:
            for var in self.variables:
                file_obj = cdms2.open(var.uri)

                self.datasets.append(DataSet(file_obj, var.uri, var.var_name))
        except cdms2.CDMSError as e:
            self.close()

            raise base.AccessError(var.uri, e.message)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def sorted(self, limit=None):
        self.datasets = sorted(self.datasets, key=lambda x: x.get_time().units)

        if limit is not None:
            for x in xrange(limit, len(self.datasets)):
                self.datasets[x].close()

            self.datasets = self.datasets[:limit]

        n = len(self.datasets)

        for i, ds in enumerate(self.datasets):
            yield round((i+1.0)*100.0/n, 2), ds

    def partitions(self, axis, limit=None):
        if limit is not None:
            for x in xrange(limit, len(self.datasets)):
                self.datasets[x].close()

            self.datasets = self.datasets[:limit]

        dataset = self.datasets[0]

        axis_index = dataset.file_obj[dataset.variable_name].getAxisIndex(axis)

        if axis_index == -1:
            raise base.WPSError('Error generating partitions, could not find axis')

        axis_data = dataset.file_obj[dataset.variable_name].getAxis(axis_index)

        if axis_data.isTime():
            axis_partition = dataset.file_obj[dataset.variable_name].getAxis(axis_index+1).id
        else:
            axis_partition = dataset.get_time().id

        for chunk in zip(*[x.partitions(axis_partition) for x in self.datasets]):
            yield chunk

    def close(self):
        for ds in self.datasets:
            ds.close()

        self.datasets = []

    def __del__(self):
        self.close()
