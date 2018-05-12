#! /usr/bin/env python

import cdms2
import cwt
import hashlib
import json
import math
import mock
import random
from django import test

from . import helpers
from wps import models
from wps import settings
from wps import tasks
from wps.tasks import file_manager

class DataSetTestCase(test.TestCase):

    def setUp(self):
        self.ds = file_manager.DataSet(cwt.Variable('file:///test.nc', 'tas'))

    def test_close(self):
        self.ds.file_obj = mock.MagicMock()

        self.ds.temporal_axis = mock.MagicMock()

        mock_lat = mock.MagicMock()

        self.ds.spatial['lat'] = mock_lat

        self.ds.variable = mock.MagicMock()

        self.ds.close()

        self.assertIsNone(self.ds.file_obj)

        self.assertIsNone(self.ds.temporal_axis)

        self.assertEqual(self.ds.spatial_axis, {})

        self.assertIsNone(self.ds.variable)
        
    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_open_error(self, mock_open):
        mock_open.side_effect = cdms2.CDMSError('some error')

        with self.assertRaises(tasks.AccessError):
            self.ds.open()

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_open(self, mock_open):
        self.ds.open()

        mock_open.assert_called_with('file:///test.nc')

    def test_map_domain_missing(self):
        mock_var = mock.MagicMock()
        
        mock_var.getTime.return_value.shape = (200,)

        mock_axis1 = mock.MagicMock()
        mock_axis1.isTime.return_value = True

        mock_axis2 = mock.MagicMock()
        mock_axis2.isTime.return_value = False
        mock_axis2.id = 'lat'
        mock_axis2.shape = (180,)
        mock_axis2.__getitem__.side_effect = [0, 180]

        mock_var.getAxisList.return_value = [
            mock_axis1,
            mock_axis2,
        ]

        self.ds.variable = mock_var

        self.ds.map_domain(None, 'days since 1990')

        self.assertEqual(self.ds.temporal, slice(0, 200))

        self.assertIn('lat', self.ds.spatial)
        self.assertEqual(self.ds.spatial['lat'], (0, 180))

    def test_map_domain_missing_axis(self):
        domain = cwt.Domain([
            cwt.Dimension('time', 20, 200, cwt.INDICES),
            cwt.Dimension('lat', 45, -45),
        ])

        mock_var = mock.MagicMock()

        mock_var.getAxisIndex.side_effect = [0, -1]

        mock_axis1 = mock.MagicMock()
        mock_axis1.isTime.return_value = True
        mock_axis1.id = 'time'
        mock_axis1.shape = (200,)

        mock_axis2 = mock.MagicMock()
        mock_axis2.isTime.return_value = False
        mock_axis2.id = 'lat'
        mock_axis2.shape = (180,)
        
        mock_var.getAxis.side_effect = [
            mock_axis1,
            mock_axis2
        ]

        self.ds.variable = mock_var

        with self.assertRaises(tasks.WPSError):
            self.ds.map_domain(domain, 'days since 1990')

    def test_map_domain(self):
        domain = cwt.Domain([
            cwt.Dimension('time', 20, 200, cwt.INDICES),
            cwt.Dimension('lat', 45, -45),
        ])

        mock_var = mock.MagicMock()

        mock_var.getAxisIndex.side_effect = [0, 1]

        mock_axis1 = mock.MagicMock()
        mock_axis1.isTime.return_value = True
        mock_axis1.id = 'time'
        mock_axis1.shape = (200,)

        mock_axis2 = mock.MagicMock()
        mock_axis2.isTime.return_value = False
        mock_axis2.id = 'lat'
        mock_axis2.shape = (180,)
        
        mock_var.getAxis.side_effect = [
            mock_axis1,
            mock_axis2
        ]

        self.ds.variable = mock_var

        self.ds.map_domain(domain, 'days since 1990')

        self.assertEqual(self.ds.temporal, slice(20, 200, 1))

        self.assertIn('lat', self.ds.spatial)
        self.assertEqual(self.ds.spatial['lat'], (45, -45))

    def test_dimension_to_selector_type_error(self):
        mock_axis_clone = mock.MagicMock()

        mock_axis_clone.mapInterval.side_effect = TypeError()

        mock_axis = mock.MagicMock()

        mock_axis.isTime.return_value = True

        mock_axis.clone.return_value = mock_axis_clone

        dimension = cwt.Dimension('time', 20, 100)

        with self.assertRaises(tasks.DomainMappingError):
            selector = self.ds.dimension_to_selector(dimension, mock_axis, 'days since 1990')

    def test_dimension_to_selector_spatial(self):
        mock_axis_clone = mock.MagicMock()

        mock_axis_clone.mapInterval.return_value = (2, 5)

        mock_axis = mock.MagicMock()

        mock_axis.isTime.return_value = False

        mock_axis.clone.return_value = mock_axis_clone

        dimension = cwt.Dimension('time', 20, 100)

        selector = self.ds.dimension_to_selector(dimension, mock_axis, 'days since 1990')

        self.assertEqual(selector, (20, 100))

    def test_dimension_to_selector_indices(self):
        mock_axis_clone = mock.MagicMock()

        mock_axis_clone.mapInterval.return_value = (2, 5)

        mock_axis = mock.MagicMock()

        mock_axis.shape = (80,)

        mock_axis.isTime.return_value = True

        mock_axis.clone.return_value = mock_axis_clone

        dimension = cwt.Dimension('time', 20, 100, cwt.INDICES)

        selector = self.ds.dimension_to_selector(dimension, mock_axis, 'days since 1990')

        self.assertEqual(selector, slice(20, 80, 1))

        self.assertEqual(dimension.start, 0)

        self.assertEqual(dimension.end, 20)

    def test_dimension_to_selector_unknown_crs(self):
        mock_axis_clone = mock.MagicMock()

        mock_axis_clone.mapInterval.return_value = (2, 5)

        mock_axis = mock.MagicMock()

        mock_axis.isTime.return_value = True

        mock_axis.clone.return_value = mock_axis_clone

        dimension = cwt.Dimension('time', 20, 100, cwt.CRS('new'))

        with self.assertRaises(tasks.WPSError):
            selector = self.ds.dimension_to_selector(dimension, mock_axis, 'days since 1990')

    def test_dimension_to_selector(self):
        mock_axis_clone = mock.MagicMock()

        mock_axis_clone.mapInterval.return_value = (2, 5)

        mock_axis = mock.MagicMock()

        mock_axis.isTime.return_value = True

        mock_axis.clone.return_value = mock_axis_clone

        dimension = cwt.Dimension('time', 20, 100)

        selector = self.ds.dimension_to_selector(dimension, mock_axis, 'days since 1990')

        self.assertEqual(selector, slice(2, 5))

        mock_axis_clone.toRelativeTime.assert_called_with('days since 1990')

        mock_axis_clone.mapInterval.assert_called_with((20, 100))

    def test_partitions_from_slice(self):
        mock_axis = mock.MagicMock()

        mock_axis.isTime.return_value = True

        self.ds.temporal = slice(0, 22)

        self.ds.get_axis = mock.Mock(return_value=mock_axis)

        self.ds.variable = mock.MagicMock()

        partitions = [x for x in self.ds.partitions('time')]

        self.ds.variable.assert_has_calls([
            mock.call(time=slice(0, 10)),
            mock.call(time=slice(10, 20)),
            mock.call(time=slice(20, 22))
        ])

    def test_partitions_spatial(self):
        mock_axis = mock.MagicMock()

        mock_axis.id = 'lat'

        mock_axis.isTime.return_value = False

        mock_axis.shape = (32,)

        self.ds.get_axis = mock.Mock(return_value=mock_axis)

        self.ds.variable = mock.MagicMock()

        partitions = [x for x in self.ds.partitions('lat')]

        self.ds.variable.assert_has_calls([
            mock.call(lat=slice(0, 10)),
            mock.call(lat=slice(10, 20)),
            mock.call(lat=slice(20, 30)),
            mock.call(lat=slice(30, 32)),
        ])

    def test_partitions_cache(self):
        mock_axis = mock.MagicMock()

        mock_axis.isTime.return_value = True

        mock_axis.shape = (22,)

        self.ds.cache_obj = mock.MagicMock()

        self.ds.get_axis = mock.Mock(return_value=mock_axis)

        self.ds.variable = mock.MagicMock()

        partitions = [x for x in self.ds.partitions('time')]

        self.ds.variable.assert_has_calls([
            mock.call(time=slice(0, 10)),
            mock.call(time=slice(10, 20)),
            mock.call(time=slice(20, 22))
        ])

        self.ds.cache_obj.assert_has_calls([

        ])

    def test_partitions(self):
        mock_axis = mock.MagicMock()

        mock_axis.isTime.return_value = True

        mock_axis.shape = (22,)

        self.ds.get_axis = mock.Mock(return_value=mock_axis)

        self.ds.variable = mock.MagicMock()

        partitions = [x for x in self.ds.partitions('time')]

        self.ds.variable.assert_has_calls([
            mock.call(time=slice(0, 10)),
            mock.call(time=slice(10, 20)),
            mock.call(time=slice(20, 22))
        ])

    def test_get_variable_missing(self):
        mock_var = mock.MagicMock()

        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value = mock_var

        self.ds.file_obj = mock_file

        with self.assertRaises(tasks.WPSError):
            variable = self.ds.get_variable()
            
    def test_get_variable_cached(self):
        mock_var = mock.MagicMock()

        self.ds.variable = mock_var

        variable = self.ds.get_variable()

        self.assertEqual(variable, mock_var)

    def test_get_variable(self):
        mock_var = mock.MagicMock()

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value = mock_var

        self.ds.file_obj = mock_file

        variable = self.ds.get_variable()

        self.assertEqual(variable, mock_var)
        
    def test_get_axis_spatial_from_file_missing(self):
        mock_axis = mock.MagicMock()

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value.getAxisIndex.return_value = -1

        mock_file.__getitem__.return_value.getAxis.return_value = mock_axis

        self.ds.file_obj = mock_file

        with self.assertRaises(tasks.WPSError):
            axis = self.ds.get_axis('lat')
        
    def test_get_axis_spatial_from_file(self):
        mock_axis = mock.MagicMock()

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value.getAxisIndex.return_value = 1

        mock_file.__getitem__.return_value.getAxis.return_value = mock_axis

        self.ds.file_obj = mock_file

        axis = self.ds.get_axis('lat')
         
        self.assertEqual(axis, mock_axis)

    def test_get_axis_spatial(self):
        mock_axis = mock.MagicMock(id='lat')

        self.ds.spatial_axis['lat'] = mock_axis

        axis = self.ds.get_axis('lat')
         
        self.assertEqual(axis, mock_axis)

    def test_get_axis(self):
        self.ds.temporal_axis = mock.MagicMock()

        self.ds.temporal_axis.id = 'time'

        axis = self.ds.get_axis('time')
         
        self.assertEqual(axis, self.ds.temporal_axis)

    def test_get_time_access_error(self):
        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value.getTime.side_effect = cdms2.CDMSError('some error')

        self.ds.file_obj = mock_file

        with self.assertRaises(tasks.AccessError):
            time = self.ds.get_time()

    def test_get_time_from_file(self):
        mock_time = mock.MagicMock()

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value.getTime.return_value = mock_time

        self.ds.file_obj = mock_file

        time = self.ds.get_time()

        self.assertEqual(time, mock_time)

    def test_get_time(self):
        self.ds.temporal_axis = mock.MagicMock()

        time = self.ds.get_time()

        self.assertEqual(time, self.ds.temporal_axis)

    def test_equal_wrong_type(self):
        class Something:
            pass

        new_ds = Something()

        self.assertNotEqual(self.ds, new_ds)

    def test_equal_different(self):
        new_ds = file_manager.DataSet(cwt.Variable('file:///test.nc', 'cct'))

        self.assertNotEqual(self.ds, new_ds)

    def test_equal(self):
        self.assertEqual(self.ds, self.ds)

class DateSetCollectionTestCase(test.TestCase):

    def test_partitions(self):
        domain = cwt.Domain([
            cwt.Dimension('time', 20, 100),
            cwt.Dimension('lat', 0, 90),
        ])

        mock_cache = mock.MagicMock()

        mock_cache_obj = mock.MagicMock()

        collection = file_manager.DataSetCollection()

        collection.check_cache = mock.MagicMock(return_value=(mock_cache, mock_cache_obj))

        mock_dataset1 = mock.MagicMock()
        mock_dataset1.variable_name = 'tas'
        mock_dataset1.get_time.return_value.id = 'time'
        mock_dataset1.get_time.return_value.units = 'days since 2000'
        mock_dataset1.partitions.return_value = [
            mock.MagicMock(),
        ]

        mock_dataset2 = mock.MagicMock()
        mock_dataset2.variable_name = 'tas'
        mock_dataset2.get_time.return_value.id = 'time'
        mock_dataset2.get_time.return_value.units = 'days since 1990'
        mock_dataset2.partitions.return_value = [
            mock.MagicMock(),
        ]

        collection.datasets = [
            mock_dataset1,
            mock_dataset2,
        ]

        result = [x for x in collection.partitions(domain, False)]

        mock_dataset1.partitions.return_value[0].getTime.return_value.toRelativeTime.assert_called_with('days since 1990')
        mock_dataset2.partitions.return_value[0].getTime.return_value.toRelativeTime.assert_called_with('days since 1990')

        mock_cache_obj.write.assert_has_calls([
            mock.call(mock_dataset2.partitions.return_value[0], id='tas'),
            mock.call(mock_dataset1.partitions.return_value[0], id='tas'),
        ])

        mock_cache_obj.sync.assert_called()

        mock_cache.set_size.assert_called()

        mock_dataset1.map_domain.assert_called_with(domain, 'days since 1990')
        mock_dataset2.map_domain.assert_called_with(domain, 'days since 1990')

        self.assertEqual(collection.datasets[0], mock_dataset2)
        self.assertEqual(collection.datasets[1], mock_dataset1)

        self.assertEqual(result[0], (mock_dataset2, mock_dataset2.partitions.return_value[0]))
        self.assertEqual(result[1], (mock_dataset1, mock_dataset1.partitions.return_value[0]))

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_replace_file_obj(self, mock_open):
        uid_hash = hashlib.sha256('file:///test1.nc:tas').hexdigest()

        cache_entry = models.Cache.objects.create(uid=uid_hash, url='file:///test1.nc', dimensions='{}')

        ds = file_manager.DataSet(cwt.Variable('file:///test1.nc', 'tas'))

        collection = file_manager.DataSetCollection()

        collection.get_cache_entry = mock.MagicMock(return_value=cache_entry)

        with self.assertNumQueries(0):
            result = collection.check_cache(ds)
            
        mock_open.assert_called_with(cache_entry.local_path, 'r')

        self.assertIsNone(result)

        self.assertEqual(ds.file_obj, mock_open.return_value)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_error_opening(self, mock_open):
        mock_open.side_effect = cdms2.CDMSError('some error')

        ds = file_manager.DataSet(cwt.Variable('file:///test1.nc', 'tas'))

        collection = file_manager.DataSetCollection()

        collection.get_cache_entry = mock.MagicMock(return_value=None)

        with self.assertNumQueries(2):
            result = collection.check_cache(ds)
            
        self.assertIsNone(result)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache(self, mock_open):
        ds = file_manager.DataSet(cwt.Variable('file:///test1.nc', 'tas'))

        collection = file_manager.DataSetCollection()

        collection.get_cache_entry = mock.MagicMock(return_value=None)

        with self.assertNumQueries(1):
            cache, cache_obj = collection.check_cache(ds)
            
        mock_open.assert_called_with(cache.local_path, 'w')

        self.assertIsNotNone(cache_obj)

    @mock.patch('wps.tasks.file_manager.models.Cache.is_superset')
    @mock.patch('wps.tasks.file_manager.models.Cache.valid')
    def test_get_cache_entry_valid(self, mock_valid, mock_superset):
        mock_superset.return_value = True

        mock_valid.return_value = True

        uid_hash = hashlib.sha256('file:///test1.nc:tas')

        models.Cache.objects.create(uid=uid_hash, url='file:///test1.nc', dimensions=json.dumps({}))

        collection = file_manager.DataSetCollection()
       
        entry = collection.get_cache_entry(uid_hash, {})

        self.assertIsNotNone(entry)

    @mock.patch('wps.tasks.file_manager.models.Cache.is_superset')
    @mock.patch('wps.tasks.file_manager.models.Cache.valid')
    def test_get_cache_entry_valid(self, mock_valid, mock_superset):
        mock_superset.return_value = False

        mock_valid.return_value = True

        uid_hash = hashlib.sha256('file:///test1.nc:tas')

        models.Cache.objects.create(uid=uid_hash, url='file:///test1.nc', dimensions=json.dumps({}))

        collection = file_manager.DataSetCollection()
       
        entry = collection.get_cache_entry(uid_hash, {})

        self.assertIsNone(entry)

    def test_get_cache_entry_invalid(self):
        uid_hash = hashlib.sha256('file:///test1.nc:tas')

        models.Cache.objects.create(uid=uid_hash, url='file:///test1.nc', dimensions=json.dumps({}))

        collection = file_manager.DataSetCollection()
       
        entry = collection.get_cache_entry(uid_hash, {})

        self.assertIsNone(entry)

    def test_get_cache_entry(self):
        uid_hash = hashlib.sha256('file:///test1.nc:tas')

        collection = file_manager.DataSetCollection()
       
        entry = collection.get_cache_entry(uid_hash, {})

        self.assertIsNone(entry)

    def test_generate_dataset_domain_slices(self):
        collection = file_manager.DataSetCollection()

        mock_dataset = mock.MagicMock()

        mock_dataset.variable_name = 'tas'

        mock_dataset.temporal = slice(1, 20)

        mock_dataset.spatial = {
            'lat': slice(20, 40),
            'lon': slice(0, 100)
        }

        domain = collection.generate_dataset_domain(mock_dataset)

        expected = {
            'variable': 'tas',
            'temporal': slice(1, 20),
            'spatial': {
                'lat': slice(20, 40),
                'lon': slice(0, 100)
            }
        }

        self.assertEqual(domain, expected)

    def test_generate_dataset_domain(self):
        collection = file_manager.DataSetCollection()

        mock_dataset = mock.MagicMock()

        mock_dataset.variable_name = 'tas'

        mock_dataset.temporal = (20, 202)

        mock_dataset.spatial = {
            'lat': (-45, 45),
            'lon': (0, 180)
        }

        mock_dataset.get_time.return_value.mapInterval.return_value = (1, 20)

        mock_dataset.get_axis.return_value.mapInterval.side_effect = [
            (20, 40),
            (0, 100),
        ]

        domain = collection.generate_dataset_domain(mock_dataset)

        expected = {
            'variable': 'tas',
            'temporal': slice(1, 20),
            'spatial': {
                'lat': slice(20, 40),
                'lon': slice(0, 100)
            }
        }

        self.assertEqual(domain, expected)

    def test_get_base_units(self):
        collection = file_manager.DataSetCollection()

        mock_dataset = mock.MagicMock()

        mock_dataset.get_time.return_value.units = 'days since 1990'

        collection.datasets.append(mock_dataset)

        base_units = collection.get_base_units()

        self.assertEqual(base_units, 'days since 1990')

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_context_manager(self, mock_open):
        collection = file_manager.DataSetCollection()
        
        collection.add(cwt.Variable('file:///test1.nc', 'tas'))
        collection.add(cwt.Variable('file:///test1.nc', 'tas'))
        collection.add(cwt.Variable('file:///test1.nc', 'tas'))

        with collection as collection:
            pass

        self.assertEqual(3, mock_open.call_count)
        self.assertEqual(3, mock_open.return_value.close.call_count)

    def test_add(self):
        collection = file_manager.DataSetCollection()
        
        collection.add(cwt.Variable('file:///test1.nc', 'tas'))
        collection.add(cwt.Variable('file:///test1.nc', 'tas'))
        collection.add(cwt.Variable('file:///test1.nc', 'tas'))

        self.assertEqual(len(collection.datasets), 3)

class FileManagerTestCase(test.TestCase):

    def test_partitions_spatial(self):
        fm = file_manager.FileManager([])

        def partitions():
            for x in xrange(2):
                yield 'part {}'.format(x)

        dsc1 = mock.MagicMock()
        dsc1.partitions.return_value = partitions()

        mock_axis = mock.MagicMock()
        
        mock_axis.isTime.return_value = False

        dsc1.datasets.__getitem__.return_value.get_axis.return_value = mock_axis

        dsc1.datasets.__getitem__.return_value.get_time.return_value.id = 'time'

        dsc2 = mock.MagicMock()
        dsc2.partitions.return_value = partitions()

        fm.collections.append(dsc1)
        fm.collections.append(dsc2)

        domain = cwt.Domain([
            cwt.Dimension('time', 20, 200),
            cwt.Dimension('lat', 0, 90),
        ])

        result = [x for x in fm.partitions(domain, 'lat')]

        dsc1.partitions.assert_called_with(domain, False, 'time')

        dsc1.datasets.__getitem__.return_value.get_time.assert_called()
        dsc1.datasets.__getitem__.return_value.get_axis.assert_called()

        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0]), 2)

    def test_partitions_limit(self):
        fm = file_manager.FileManager([])

        def partitions():
            for x in xrange(2):
                yield 'part {}'.format(x)

        dsc1 = mock.MagicMock()
        dsc1.partitions.return_value = partitions()

        dsc1.datasets.__getitem__.return_value.get_time.return_value.isTime.return_value = True

        mock_axis = mock.MagicMock(id='lat')

        dsc1.datasets.__getitem__.return_value.get_variable.return_value.getAxis.return_value = mock_axis

        dsc2 = mock.MagicMock()
        dsc2.partitions.return_value = partitions()

        fm.collections.append(dsc1)
        fm.collections.append(dsc2)

        domain = cwt.Domain([
            cwt.Dimension('time', 20, 200),
            cwt.Dimension('lat', 0, 90),
        ])

        result = [x for x in fm.partitions(domain, limit=1)]

        dsc1.partitions.assert_called_with(domain, True, 'lat')

        dsc1.datasets.__getitem__.return_value.get_time.assert_called()
        dsc1.datasets.__getitem__.return_value.get_axis.assert_not_called()

        dsc2.close.assert_called()

        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0]), 1)

    def test_partitions(self):
        fm = file_manager.FileManager([])

        def partitions():
            for x in xrange(2):
                yield 'part {}'.format(x)

        dsc1 = mock.MagicMock()
        dsc1.partitions.return_value = partitions()

        dsc1.datasets.__getitem__.return_value.get_time.return_value.isTime.return_value = True

        mock_axis = mock.MagicMock(id='lat')

        dsc1.datasets.__getitem__.return_value.get_variable.return_value.getAxis.return_value = mock_axis

        dsc2 = mock.MagicMock()
        dsc2.partitions.return_value = partitions()

        fm.collections.append(dsc1)
        fm.collections.append(dsc2)

        domain = cwt.Domain([
            cwt.Dimension('time', 20, 200),
            cwt.Dimension('lat', 0, 90),
        ])

        result = [x for x in fm.partitions(domain)]

        dsc1.partitions.assert_called_with(domain, True, 'lat')

        dsc1.datasets.__getitem__.return_value.get_time.assert_called()
        dsc1.datasets.__getitem__.return_value.get_axis.assert_not_called()

        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0]), 2)

    def test_get_variable_name(self):
        fm = file_manager.FileManager([])

        dsc = file_manager.DataSetCollection()

        dsc.datasets = mock.MagicMock()

        dsc.datasets.__getitem__.return_value.variable_name = 'tas'

        fm.collections.append(dsc)

        var_name = fm.get_variable_name()

        self.assertEqual(var_name, 'tas')

    def test_context_manager(self):
        fm = file_manager.FileManager([
            file_manager.DataSetCollection.from_variables([cwt.Variable('file:///test1.nc', 'tas')]),
            file_manager.DataSetCollection.from_variables([cwt.Variable('file:///test2.nc', 'tas')]),
        ])

        with fm as fm:
            self.assertEqual(len(fm.collections), 2)

            self.assertEqual(len(fm.collections[0].datasets), 1)
