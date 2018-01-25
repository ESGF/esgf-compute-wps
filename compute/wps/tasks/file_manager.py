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

        self.url = variable.uri

        self.variable_name = variable.var_name

        self.variable = None

        self.temporal_axis = None

        self.spatial_axis = {}

        self.temporal = None

        self.spatial = {}

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

    def get_time(self):
        if self.temporal_axis is None:
            try:
                self.temporal_axis = self.get_variable().getTime()
            except cdms2.CDMSError as e:
                raise base.AccessError(self.url, e.message)

        return self.temporal_axis

    def get_axis(self, name):
        if self.temporal_axis is not None and self.temporal_axis.id == name:
            return self.temporal_axis

        if name in self.spatial_axis:
            return self.spatial_axis[name]

        axis_index = self.get_variable().getAxisIndex(name)

        if axis_index == -1:
            raise base.WPSError('Axis "{name}" does not exist', name=name)

        axis = self.get_variable().getAxis(axis_index)

        if axis.isTime():
            self.temporal_axis = axis
        else:
            self.spatial_axis[name] = axis

        return axis

    def get_variable(self):
        if self.variable is None:
            if self.variable_name not in self.file_obj.variables:
                raise base.WPSError('File "{url}" does not contain variable "{var_name}"', url=self.file_obj.id, var_name=self.variable_name)

            self.variable = self.file_obj[self.variable_name]

        return self.variable

    def partitions(self, axis_name):
        axis = self.get_axis(axis_name)

        if axis.isTime():
            if self.temporal is None:
                start = 0

                stop = axis.shape[0]
            elif isinstance(self.temporal, slice):
                start = self.temporal.start

                stop = self.temporal.stop

            diff = stop - start

            step = min(diff, settings.PARTITION_SIZE)

            for begin in xrange(start, stop, step):
                end = min(begin + step, stop)

                data = self.get_variable()(time=slice(begin, end), **self.spatial)

                yield data
        else:
            start = 0

            stop = axis.shape[0]

            diff = stop - start

            step = min(diff, settings.PARTITION_SIZE)

            for begin in xrange(start, stop, step):
                end = min(begin + step, stop)

                data = self.get_variable()(**{axis_name: (begin, end)})

                yield data

    def dimension_to_selector(self, dimension, axis, base_units=None):
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
            if variable.getTime() == None:
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
                    self.temporal = self.dimension_to_selector(dim, axis, base_units)

                    logger.info('Mapped temporal domain to "{}"'.format(self.temporal))
                else:
                    self.spatial[dim.name] = self.dimension_to_selector(dim, axis)

                    logger.info('Mapped spatial "{}" to "{}"'.format(dim.name, self.spatial[dim.name]))

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

    def generate_dataset_domain(self, dataset):
        domain = {
            'variable': dataset.variable_name,
            'temporal': None,
            'spatial': {}
        }

        if isinstance(dataset.temporal, (list, tuple)):
            indices = dataset.get_time().mapInterval(dataset.temporal)

            domain['temporal'] = slice(indices[0], indices[1])
        else:
            domain['temporal'] = dataset.temporal

        for name in dataset.spatial.keys():
            spatial = dataset.spatial[name]

            if isinstance(spatial, (list, tuple)):
                indices = dataset.get_axis(name).mapInterval(spatial)

                domain['spatial'][name] = slice(indices[0], indices[1])
            else:
                domain['spatial'][name] = spatial

        return domain

    def get_cache_entry(self, uid_hash, domain):
        cache_entries = models.Cache.objects.filter(uid=uid_hash)

        logger.info('Found "{}" cache entries matching hash "{}"'.format(len(cache_entries), uid_hash))

        for x in xrange(cache_entries.count()):
            entry = cache_entries[x]

            logger.info('Checking cache entry with dimensions "{}"'.format(entry.dimensions))
            
            if not entry.valid:
                logger.info('Cache entry hash "{}" with dimensions "{}" invalid'.format(entry.uid, entry.dimensions))

                entry.delete()
                
                continue

            if entry.is_superset(domain):
                logger.info('Cache entry is a superset of the requested domain')

                return entry

        return None

    def check_cache(self, dataset):
        uid = '{}:{}'.format(dataset.url, dataset.variable_name)

        uid_hash = hashlib.sha256(uid).hexdigest()

        domain = self.generate_dataset_domain(dataset)

        logger.info('Checking cache for "{}" with domain "{}"'.format(uid_hash, domain))

        cache = self.get_cache_entry(uid_hash, domain)

        if cache is None:
            logger.info('Creating cache file for "{}"'.format(dataset.url))

            dimensions = json.dumps(domain, default=models.slice_default)

            cache = models.Cache.objects.create(uid=uid_hash, url=dataset.url, dimensions=dimensions)

            mode = 'w'
        else:
            logger.info('Using cache file "{}" as input'.format(cache.local_path))

            mode = 'r'

        try:
            cache_obj = cdms2.open(cache.local_path, mode)
        except cdms2.CDMSError as e:
            logger.exception('Error opening cache file "{}"'.format(cache.local_path))

            cache.delete()

            return None

        if mode == 'r':
            dataset.close()

            dataset.file_obj = cache_obj

            return None
                
        return cache, cache_obj

    def partitions(self, domain, skip_cache, axis=None):
        logger.info('Sorting datasets')

        self.datasets = sorted(self.datasets, key=lambda x: x.get_time().units)

        base_units = self.datasets[0].get_time().units

        if axis is None:
            axis = self.datasets[0].get_time().id

        logger.info('Generating paritions over axis "{}" caching {}'.format(axis, not skip_cache))

        base_units = None

        for ds in self.datasets:
            cache = None
            cache_obj = None

            if base_units is None:
                base_units = ds.get_time().units

            try:
                ds.map_domain(domain, base_units)
            except DomainMappingError:
                logger.info('Skipping "{}"'.format(ds.url))

                continue

            if not skip_cache:
                cache_result = self.check_cache(ds)

                if cache_result is not None:
                    cache, cache_obj = cache_result

            for chunk in ds.partitions(axis):
                if cache_obj is not None:
                    cache_obj.write(chunk, id=ds.variable_name)

                    cache_obj.sync()

                chunk.getTime().toRelativeTime(base_units)

                yield ds, chunk

            if cache is not None:
                cache.set_size()

class FileManager(object):
    def __init__(self, variables):
        self.variables = variables

        self.collections = []

    def __enter__(self):
        collection = None
        common = 0
        last = 0

        logger.info('Grouping {} datasets'.format(len(self.variables)))

        for x in xrange(len(self.variables)):
            curr = self.variables[x].uri

            curr_split = curr.split('/')

            curr_path = '/'.join(curr_split[:len(curr_split)-1])

            prev = self.variables[x-1].uri

            prev_split = prev.split('/')

            prev_path = '/'.join(prev_split[:len(prev_split)-1])

            n = min(len(curr_path), len(prev_path))

            logger.info('Comparing "{}" {} to "{}" {}, minimum length {}'.format(curr_path, len(curr_path), prev_path, len(prev_path), n))

            common = 0

            for y in xrange(n):
                if curr_path[y] != prev_path[y]:
                    break

                common += 1

            logger.info('Found {} characters in common, last {}'.format(common, last))

            if common == n:
                if collection is None:
                    collection = DataSetCollection()

                collection.add(self.variables[x]) 
            else:
                if collection is not None:
                    self.collections.append(collection)

                collection = DataSetCollection()

                collection.add(self.variables[x])

                last = common


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

        dataset = collection[0]

        if axis is None:
            axis_data = dataset.get_time()
        else:
            axis_data = dataset.get_axis(axis)

        skip_cache = False

        if axis_data.isTime():
            axis_index = dataset.get_variable().getAxisIndex(axis_data.id)

            axis_partition = dataset.get_variable().getAxis(axis_index+1).id

            skip_cache = True
        else:
            axis_partition = dataset.get_time().id

        # Was using zip to but it was build entire list of values from generator
        try:
            gens = [x.partitions(domain, skip_cache, axis_partition) for x in self.collections]

            while True:
                chunks = [x.next() for x in gens]

                yield chunks
        except StopIteration:
            pass
