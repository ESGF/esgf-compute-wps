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

    def estimate_axis_size(self, axis, domain):
        if domain is None:
            return axis.shape[0]
        else:
            if isinstance(domain, slice):
                return (domain.stop - domain.start) / (domain.step or 1)
            elif isinstance(domain, (tuple, list)):
                indices = axis.mapInterval(domain)

                return indices[1] - indices[0]

    def estimate_size(self):
        dimensions = []

        for axis in self.variable.getAxisList():
            if axis.isTime():
                dimensions.append(self.estimate_axis_size(axis, self.temporal))
            else:
                dimensions.append(self.estimate_axis_size(axis, self.spatial.get(axis.id, None)))

        dtype = self.get_variable().dtype

        logger.info('Dimension %r', dimensions)

        estimate = reduce(lambda x, y: x * y, dimensions)

        estimate = estimate + sum(dimensions)

        estimate = estimate * dtype.itemsize

        return estimate / 1048576.0

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

    def partitions(self, axis_name, skip_data=False):
        axis = self.get_axis(axis_name)

        logger.info('Generating partitions over axis %s', axis_name)

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

                logger.info('Chunk temporal %r spatial %r', slice(begin, end), self.spatial)

                if skip_data:
                    yield { 'temporal': slice(begin, end), 'spatial': self.spatial }
                else:
                    data = self.get_variable()(time=slice(begin, end), **self.spatial)

                    yield data
        else:
            domain_axis = self.spatial.get(axis.id, None)

            if domain_axis is None:
                start = 0

                stop = axis.shape[0]
            else:
                start, stop = axis.mapInterval(domain_axis)

            diff = stop - start

            step = min(diff, settings.PARTITION_SIZE)

            for begin in xrange(start, stop, step):
                end = min(begin + step, stop)

                self.spatial[axis.id] = slice(begin, end)
                #self.spatial[axis_name] = slice(begin, end)
                #self.spatial[axis_name] = (begin, end)

                logger.info('Chunk temporal %r spatial %r', slice(begin, end), self.spatial)

                if skip_data:
                    yield { 'temporal': self.temporal, 'spatial': self.spatial }
                else:
                    data = self.get_variable()(**self.spatial)
                    #data = self.get_variable()(**{axis_name: slice(begin, end)})

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
                time = variable.getTime()

                #self.temporal = (time[0], time[-1])
                self.temporal = slice(0, time.shape[0])
                #self.temporal = slice(0, variable.getTime().shape[0])

            axes = variable.getAxisList()

            for axis in axes:
                if axis.isTime():
                    continue

                self.spatial[axis.id] = (axis[0], axis[-1])
                #self.spatial[axis.id] = slice(0, axis.shape[0])
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
                    self.spatial[axis.id] = self.dimension_to_selector(dim, axis)
                    #self.spatial[dim.name] = self.dimension_to_selector(dim, axis)

                    logger.info('Mapped spatial "{}" to "{}"'.format(axis.id, self.spatial[axis.id]))
                    #logger.info('Mapped spatial "{}" to "{}"'.format(dim.name, self.spatial[dim.name]))

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
    def __init__(self, datasets=None):
        if datasets is None:
            datasets = []

        self.datasets = datasets

    @classmethod
    def from_variables(cls, variables):
        datasets = [DataSet(x) for x in variables]

        return cls(datasets=datasets)

    def __enter__(self):
        for ds in self.datasets:
            ds.open()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for ds in self.datasets:
            ds.close()

    def estimate_size(self, domain):
        base_units = self.get_base_units()

        estimate = 0.0

        for x in self.datasets:
            x.map_domain(domain, base_units)

            estimate += x.estimate_size()

        return estimate

    def add(self, variable):
        self.datasets.append(DataSet(variable))

    def sort_datasets(self):
        self.datasets = sorted(self.datasets, key=lambda x: x.get_time().units)

    def get_base_units(self):
        self.sort_datasets()

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

    def get_cache_entry(self, dataset, domain):
        uid = '{}:{}'.format(dataset.url, dataset.variable_name)

        uid_hash = hashlib.sha256(uid).hexdigest()

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

    def partitions(self, domain, skip_cache, axis=None, skip_data=False):
        logger.info('Sorting datasets')

        self.datasets = sorted(self.datasets, key=lambda x: x.get_time().units)

        if axis is None:
            axis = self.datasets[0].get_time().id

        base_units = None

        for dataset in self.datasets:
            logger.info('Generating partitions for %s over %s axis, caching %s', dataset.url, axis, not skip_cache) 

            cache = None
            cache_obj = None

            if base_units is None:
                base_units = dataset.get_time().units

            try:
                dataset.map_domain(domain, base_units)
            except DomainMappingError:
                logger.info('Skipping "{}"'.format(dataset.url))

                continue

            if not skip_cache:
                dataset_domain = self.generate_dataset_domain(dataset)

                cache = self.get_cache_entry(dataset, dataset_domain)

                try:
                    if cache is None:
                        logger.info('%s is not in cache', dataset.url)

                        dimensions = json.dumps(dataset_domain, default=models.slice_default)

                        uid = '{}:{}'.format(dataset.url, dataset.variable_name)

                        uid_hash = hashlib.sha256(uid).hexdigest()

                        cache = models.Cache.objects.create(uid=uid_hash, url=dataset.url, dimensions=dimensions)

                        cache_obj = cdms2.open(cache.local_path, 'w')
                    else:
                        logger.info('%s is in cache', dataset.url)

                        # Swap the source file for the cached file
                        dataset.close()

                        dataset.file_obj = cdms2.open(cache.local_path)

                        dataset.map_domain(domain, base_units)
                except cdms2.CDMSError as e:
                    raise base.AccessError(cache.local_path, e)

            for chunk in dataset.partitions(axis, skip_data):
                if not skip_data:
                    if cache_obj is not None:
                        cache_obj.write(chunk, id=dataset.variable_name)

                        cache_obj.sync()

                    chunk.getTime().toRelativeTime(base_units)

                yield dataset, chunk

            if cache is not None:
                cache.set_size()

class FileManager(object):
    def __init__(self, collections):
        self.collections = collections

    def __enter__(self):
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

        dataset = collection.datasets[0]

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
