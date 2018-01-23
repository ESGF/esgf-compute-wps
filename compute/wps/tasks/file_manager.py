#! /usr/bin/env python

import cdms2
import contextlib
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
    'DataSetCollection',
    'FileManager',
]

logger = get_task_logger('wps.tasks.file_manager')

class DomainMappingError(base.WPSError):
    def __init__(self):
        super(DomainMappingError, self).__init__('Error mapping domain')

class DataSet(object):
    def __init__(self, variable):
        self.file_obj = None

        self.cache_obj = None

        self.cache = None

        self.url = variable.uri

        self.variable_name = variable.var_name

        self.variable = None

        self.temporal_axis = None

        self.spatial_axis = {}

        self.temporal = None

        self.spatial = {}

        self.n = 0

    def __eq__(self, other):
        if not isinstance(other, DataSet):
            return False

        return self.variable_name == other.variable_name

    def __del__(self):
        self.close()

    def __repr__(self):
        return 'DataSet(url={url}, variable_name={variable_name}, temporal_roi={temporal}, spatial_roi={spatial})'.format(
                url=self.url,
                variable_name=self.variable_name,
                temporal=self.temporal,
                spatial=self.spatial
            )

    def partitions(self, axis_name):
        axis = None

        if (self.temporal_axis is not None and 
                self.temporal_axis.id == axis_name):
            axis = self.temporal_axis
        elif (axis_name in self.spatial_axis):
            axis = self.spatial_axis[axis_name]
        else:
            axis_index = self.get_variable().getAxisIndex(axis_name)

            if axis_index == -1:
                raise base.WPSError('Could not find axis "{name}"', name=axis_name)

            axis = self.get_variable().getAxis(axis_index)

        if axis.isTime():
            if self.temporal is None:
                start = 0

                stop = axis.shape[0]
            elif isinstance(self.temporal, slice):
                start = self.temporal.start

                stop = self.temporal.stop

            diff = stop - start

            step = min(diff, settings.PARTITION_SIZE)

            self.n = math.ceil((stop-start)/step)+1

            for begin in xrange(start, stop, step):
                end = min(begin + step, stop)

                data = self.get_variable()(time=slice(begin, end), **self.spatial)

                if self.cache_obj is not None:
                    self.cache_obj.write(data, id=self.variable_name)

                    self.cache_obj.sync()

                yield data
        else:
            start = 0

            stop = axis.shape[0]

            diff = stop - start

            step = min(diff, settings.PARTITION_SIZE)

            self.n = math.ceil((stop-start)/step) + 1

            for begin in xrange(start, stop, step):
                end = min(begin + step, stop)

                data = self.get_variable()(**{axis_name: slice(begin, end)})

                if self.cache_obj is not None:
                    self.cache_obj.write(data, id=self.variable_name)

                    self.cache_obj.sync()

                yield data

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
                    raise DomainMappingError()
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

        variable = self.get_variable()

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
                self.temporal_axis = self.get_variable().getTime()
            except cdms2.CDMSError as e:
                raise base.AccessError(self.url, e.message)

        return self.temporal_axis

    def get_axis(self, name):
        if name not in self.spatial_axis:
            axis_index = self.get_variable().getAxisIndex(name)

            if axis_index == -1:
                raise base.WPSError('Axis "{name}" does not exist', name=name)

            self.spatial_axis[name] = self.get_variable().getAxis(axis_index)

        return self.spatial_axis[name]

    def get_variable(self):
        if self.variable is None:
            if self.variable_name not in self.file_obj.variables:
                raise base.WPSError('File "{url}" does not contain variable "{var_name}"', url=self.file_obj.id, var_name=self.variable_name)

            self.variable = self.file_obj[self.variable_name]

        return self.variable

    def open(self):
        try:
            self.file_obj = cdms2.open(self.url)
        except cdms2.CDMSError as e:
            raise base.AccessError(self.url, e)

    def close(self):
        if self.variable is not None:
            del self.variable

            self.variable = None

        if self.temporal_axis is not None:
            del self.temporal_axis

            self.temporal_axis = None

        if len(self.spatial_axis) > 0:
            for name in self.spatial_axis.keys():
                del self.spatial_axis[name]

            self.spatial_axis = {}

        if self.cache_obj is not None:
            self.cache_obj.close()

            self.cache_obj = None
        
        if self.file_obj is not None:
            self.file_obj.close()

            self.file_obj = None

class DataSetCollection(object):
    def __init__(self):
        self.datasets = []

    def __enter__(self):
        for ds in self.datasets:
            ds.open()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for ds in self.datasets:
            ds.close()

    def add(self, variable):
        self.datasets.append(DataSet(variable))

    def get_base_units(self):
        return self.datasets[0].get_time().units

    def partitions(self, domain, axis=None):
        logger.info('Sorting datasets')

        self.datasets = sorted(self.datasets, key=lambda x: x.get_time().units)

        base_units = self.datasets[0].get_time().units

        if axis is None:
            axis = self.datasets[0].get_time().id

        logger.info('Generating paritions over axis "{}"'.format(axis))

        for ds in self.datasets:
            try:
                ds.map_domain(domain, base_units)
            except DomainMappingError:
                logger.info('Skipping "{}"'.format(ds.url))

                continue

            ds.check_cache()

            for chunk in ds.partitions(axis):
                yield ds.url, chunk

class FileManager(object):
    def __init__(self, variables):
        self.variables = variables

        self.collections = []

    def __enter__(self):
        collection = None
        common = 0
        last = 0

        logger.info('Grouping variables into collections')

        for x in xrange(len(self.variables)):
            curr = self.variables[x].uri

            prev = self.variables[x-1].uri

            n = min(len(curr), len(prev))

            for y in xrange(n):
                if curr[y] != prev[y]:
                    common = y

                    break

            try:
                if common == last:
                   collection.add(self.variables[x]) 
                else:
                    if collection is not None:
                        self.collections.append(collection)

                    collection = DataSetCollection()

                    collection.add(self.variables[x])

                    last = common

            except base.AccessError:
                self.close()

                raise

        self.collections.append(collection)

        logger.info('Grouped into "{}" collections'.format(len(self.collections)))

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def get_variable_name(self):
        return self.collections[0].datasets[0].variable_name

    def partitions(self, domain, axis=None, limit=None):
        if limit is not None:
            for x in xrange(limit, len(self.collections)):
                self.collections[x].close()

            self.collections = self.collections[:limit]

        collection = self.collections[0]

        with contextlib.nested(*[x for x in self.collections]):
            if axis is None:
                axis_data = collection.datasets[0].get_time()
            else:
                axis_data = collection.datasets[0].get_axis(axis)

            if axis_data.isTime():
                axis_index = collection.datasets[0].get_variable().getAxisIndex(axis_data.id)

                axis_partition = collection.datasets[0].get_variable().getAxis(axis_index+1).id
            else:
                axis_partition = collection.datasets[0].get_time().id

            for chunk in zip(*[x.partitions(axis_partition, domain) for x in self.collections]):
                yield chunk
