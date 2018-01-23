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

        mock_var.getAxisList.return_value = [
            mock_axis1,
            mock_axis2,
        ]

        self.ds.variable = mock_var

        self.ds.map_domain(None, 'days since 1990')

        self.assertEqual(self.ds.temporal, slice(0, 200))

        self.assertIn('lat', self.ds.spatial)
        self.assertEqual(self.ds.spatial['lat'], slice(0, 180))

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

        mock_axis.isTime.return_value = False

        mock_axis.shape = (32,)

        self.ds.get_axis = mock.Mock(return_value=mock_axis)

        self.ds.variable = mock.MagicMock()

        partitions = [x for x in self.ds.partitions('lat')]

        self.ds.variable.assert_has_calls([
            mock.call(lat=(0, 10)),
            mock.call(lat=(10, 20)),
            mock.call(lat=(20, 30)),
            mock.call(lat=(30, 32)),
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
    pass

class FileManagerTestCase(test.TestCase):
    pass
