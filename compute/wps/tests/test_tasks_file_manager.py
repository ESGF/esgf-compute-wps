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
from wps import WPSError

class DataSetTestCase(test.TestCase):

    def setUp(self):
        random.seed(1)

    @classmethod
    def setUpClass(cls):
        super(DataSetTestCase, cls).setUpClass()

        cls.time = helpers.generate_time('days since 1990', 365)

        cls.lat = helpers.latitude

    def test_get_axis_error(self):
        mock_file = mock.MagicMock()
        
        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value.getAxisIndex.return_value = -1

        mock_file.__getitem__.return_value.getAxis.return_value = self.lat

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        with self.assertRaises(WPSError):
            axis = ds.get_axis('lat')

    def test_get_axis(self):
        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value.getAxisIndex.return_value = 0

        mock_file.__getitem__.return_value.getAxis.return_value = self.lat

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        axis = ds.get_axis('lat')

        self.assertEqual(self.lat, axis)

    def test_partitions_missing_axis(self):
        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value.getAxisIndex.return_value = -1

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        with self.assertRaises(WPSError):
            partitions = [chunk for chunk in ds.partitions('time')]

    def test_partitions_spatial(self):
        settings.PARITION_SIZE = 20 

        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.temporal_axis = mock.MagicMock()

        ds.temporal_axis.id = 'time'

        ds.temporal_axis.isTime.return_value = True

        ds.temporal = slice(100, 200)

        ds.spatial = {'lat': slice(0, 90), 'lon': slice(0, 180)}

        # TODO finish

    def test_partitions_axis_from_file(self):
        settings.PARITION_SIZE = 10 

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value.getAxisIndex.return_value = 0

        mock_axis = mock.MagicMock()

        mock_axis.isTime.return_value = True

        mock_file.__getitem__.return_value.getAxis.return_value = mock_axis

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.temporal = slice(100, 200)

        ds.spatial = {}

        expected = [(slice(x, x+settings.PARTITION_SIZE), {}) 
                    for x in xrange(100, 200, settings.PARTITION_SIZE)]

        partitions = [chunk for chunk in ds.partitions('time')]

        self.assertEqual(expected, partitions)

    def test_partitions(self):
        settings.PARITION_SIZE = 10

        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.temporal_axis = mock.MagicMock()

        ds.temporal_axis.id = 'time'

        ds.temporal_axis.isTime.return_value = True

        ds.temporal = slice(100, 200)

        n = math.ceil((ds.temporal.stop-ds.temporal.start)/settings.PARITION_SIZE)+1

        expected = [(slice(x, x+settings.PARTITION_SIZE), {}) 
                    for i, x in enumerate(xrange(100, 200, settings.PARTITION_SIZE))]

        partitions = [chunk for chunk in ds.partitions('time')]

        self.assertEqual(expected, partitions)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists_temporal_mismatch(self, mock_open):
        variable = helpers.generate_variable([self.time, helpers.latitude, helpers.longitude], 'tas')

        uid = hashlib.sha256('file:///test.nc:tas').hexdigest()

        url = 'file:///test.nc'

        data = {
            'temporal': slice(120, 201),
            'spatial': {
                'lat': slice(100, 200),
                'lon': slice(250, 361),
            }
        }

        dimensions = json.dumps(data, default=models.slice_default)

        models.Cache.objects.create(uid=uid, url=url, dimensions=dimensions)

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.temporal = (100, 200)

        ds.spatial = {'lat': (24, 100), 'lon': (90, 180)}

        with self.assertNumQueries(3):
            ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNotNone(ds.cache)

    @mock.patch('wps.models.Cache.valid')
    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists_spatial_mismatch(self, mock_open, mock_valid):
        mock_valid.return_value = True

        variable = helpers.generate_variable([self.time, helpers.latitude, helpers.longitude], 'tas')

        uid = hashlib.sha256('file:///test.nc:tas').hexdigest()

        url = 'file:///test.nc'

        data = {
            'temporal': slice(100, 201),
            'spatial': {
                'lat': slice(120, 200),
                'lon': slice(250, 361),
            }
        }

        dimensions = json.dumps(data, default=models.slice_default)

        models.Cache.objects.create(uid=uid, url=url, dimensions=dimensions)

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.temporal = (100, 200)

        ds.spatial = {'lat': (24, 100), 'lon': (90, 180)}

        with self.assertNumQueries(2):
            ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNotNone(ds.cache)

    @mock.patch('wps.models.Cache.valid')
    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists_cached_no_domain(self, mock_open, mock_valid):
        mock_valid.return_value = True

        variable = helpers.generate_variable([self.time, helpers.latitude, helpers.longitude], 'tas')

        uid = hashlib.sha256('file:///test.nc:tas').hexdigest()

        url = 'file:///test.nc'

        data = {
            'temporal': None,
            'spatial': {
            }
        }

        dimensions = json.dumps(data, default=models.slice_default)

        models.Cache.objects.create(uid=uid, url=url, dimensions=dimensions)

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.temporal = slice(100, 200)

        ds.spatial = {
            'lat': slice(0, 90),
            'lon': slice(0, 180),
        }

        with self.assertNumQueries(1):
            ds.check_cache()

        mock_open.assert_called_once()

        mock_file.close.assert_called_once()

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists_request_no_domain(self, mock_open):
        variable = helpers.generate_variable([self.time, helpers.latitude, helpers.longitude], 'tas')

        uid = hashlib.sha256('file:///test.nc:tas').hexdigest()

        url = 'file:///test.nc'

        data = {
            'temporal': slice(100, 201),
            'spatial': {
                'lat': slice(100, 200),
                'lon': slice(250, 361),
            }
        }

        dimensions = json.dumps(data, default=models.slice_default)

        models.Cache.objects.create(uid=uid, url=url, dimensions=dimensions)

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        with self.assertNumQueries(3):
            ds.check_cache()

        mock_open.assert_called_once()

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists_error(self, mock_open):
        variable = helpers.generate_variable([self.time, helpers.latitude, helpers.longitude], 'tas')

        uid = hashlib.sha256('file:///test.nc:tas').hexdigest()

        url = 'file:///test.nc'

        data = {
            'temporal': slice(100, 201),
            'spatial': {
                'lat': slice(100, 200),
                'lon': slice(250, 361),
            }
        }

        dimensions = json.dumps(data, default=models.slice_default)

        models.Cache.objects.create(uid=uid, url=url, dimensions=dimensions)

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.temporal = (100, 200)

        ds.spatial = {'lat': (24, 100), 'lon': (90, 180)}

        mock_open.side_effect = cdms2.CDMSError('some error text')

        ds.check_cache()

        self.assertEqual(ds.file_obj, mock_file)

    @mock.patch('wps.models.Cache.valid')
    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists(self, mock_open, mock_valid):
        mock_valid.return_value = True

        variable = helpers.generate_variable([self.time, helpers.latitude, helpers.longitude], 'tas')

        uid = hashlib.sha256('file:///test.nc:tas').hexdigest()

        url = 'file:///test.nc'

        data = {
            'temporal': slice(100, 201),
            'spatial': {
                'lat': slice(100, 200),
                'lon': slice(250, 361),
            }
        }

        dimensions = json.dumps(data, default=models.slice_default)

        models.Cache.objects.create(uid=uid, url=url, dimensions=dimensions)

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.temporal = (100, 200)

        ds.spatial = {'lat': (24, 100), 'lon': (90, 180)}

        with self.assertNumQueries(1):
            ds.check_cache()

        mock_open.assert_called_once()

        mock_file.close.assert_called_once()

    @mock.patch('wps.models.Cache.valid')
    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_error_opening(self, mock_open, mock_valid):
        mock_valid.return_value = True

        mock_open.side_effect = cdms2.CDMSError('some error')

        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNone(ds.cache)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_missing_temporal(self, mock_open):
        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.spatial = {
            'lat': slice(0, 90),
            'lon': slice(0, 180),
        }

        ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNotNone(ds.cache)
        self.assertEqual(ds.cache.dimensions, 
                         '{"variable": "tas", "temporal": null, "spatial": {"lat": {"slice": "0:90:None"}, "lon": {"slice": "0:180:None"}}}')

    @mock.patch('wps.models.Cache.valid')
    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache(self, mock_open, mock_valid):
        mock_valid.return_value = True

        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        ds.temporal = slice(100, 201)

        ds.spatial = {
            'lat': slice(0, 90),
            'lon': slice(0, 180),
        }

        with self.assertNumQueries(2):
            ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNotNone(ds.cache)
        self.assertEqual(ds.cache.dimensions, 
                         '{"variable": "tas", "temporal": {"slice": "100:201:None"}, "spatial": {"lat": {"slice": "0:90:None"}, "lon": {"slice": "0:180:None"}}}')

    def test_dimension_to_cdms2_selector_indices(self):
        ds = file_manager.DataSet(mock.MagicMock(), cwt.Variable('file:///test.nc', 'tas'))

        dim = cwt.Dimension('lat', 100, 200, cwt.INDICES)

        selector = ds.dimension_to_cdms2_selector(dim, helpers.latitude, helpers.latitude.units)

        self.assertEqual(selector, slice(100, 180, 1))
        self.assertEqual(dim.start, 0)
        self.assertEqual(dim.end, 20)

    def test_dimension_to_cdms2_selector_spatial(self):
        ds = file_manager.DataSet(mock.MagicMock(), cwt.Variable('file:///test.nc', 'tas'))

        dim = cwt.Dimension('lat', 100, 200)

        selector = ds.dimension_to_cdms2_selector(dim, helpers.latitude, helpers.latitude.units)

        self.assertEqual(selector, (100, 200))

    def test_dimension_to_cdms2_selector_unknown_crs(self):
        ds = file_manager.DataSet(mock.MagicMock(), cwt.Variable('file:///test.nc', 'tas'))

        dim = cwt.Dimension('time', 100, 200, crs=cwt.CRS('test'))

        with self.assertRaises(WPSError):
            selector = ds.dimension_to_cdms2_selector(dim, self.time, self.time.units)

    def test_dimension_to_cdms2_selector(self):
        ds = file_manager.DataSet(mock.MagicMock(), cwt.Variable('file:///test.nc', 'tas'))

        dim = cwt.Dimension('time', 100, 200)

        selector = ds.dimension_to_cdms2_selector(dim, self.time, self.time.units)

        self.assertEqual(selector, slice(100, 201))

    def test_map_domain_missing_dimension(self):
        variable = helpers.generate_variable([self.time, helpers.latitude, helpers.longitude], 'tas')

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        domain = cwt.Domain([
            cwt.Dimension('time', '100', '200'),
            cwt.Dimension('lat', '0', '90'),
            cwt.Dimension('lon', '0', '180'),
            cwt.Dimension('does not exist', '10', '100'),
        ])

        with self.assertRaises(WPSError):
            ds.map_domain(domain, 'days since 1990')

    def test_map_domain(self):
        variable = helpers.generate_variable([self.time, helpers.latitude, helpers.longitude], 'tas')

        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        domain = cwt.Domain([
            cwt.Dimension('time', 100, 200),
            cwt.Dimension('lat', 0, 90),
            cwt.Dimension('lon', 0, 180),
        ])

        ds.map_domain(domain, self.time.units)

        self.assertEqual(ds.temporal, slice(100, 201))
        self.assertEqual(ds.spatial, {'lat': (0, 90), 'lon': (0, 180)})

    def test_get_time_error(self):
        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        mock_file.__getitem__.return_value.getTime.side_effect = cdms2.CDMSError('some error text')

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        with self.assertRaises(tasks.AccessError):
            time = ds.get_time()

    def test_get_time(self):
        mock_file = mock.MagicMock()

        mock_file.variables = {'tas': True}

        ds = file_manager.DataSet(mock_file, cwt.Variable('file:///test.nc', 'tas'))

        time = ds.get_time()

        mock_file.__getitem__.return_value.getTime.assert_called_once()

class FileManagerTestCase(test.TestCase):

    @classmethod
    def setUpClass(cls):
        super(FileManagerTestCase, cls).setUpClass()

        time = helpers.generate_time('days since 1990', 365)

        cls.variable = helpers.generate_variable([time, helpers.latitude, helpers.longitude], 'tas')

    def test_limit_subset(self):
        fm = file_manager.FileManager([])

        fm.datasets = [
            mock.MagicMock(),
            mock.MagicMock(),
            mock.MagicMock(),
        ]

        result = [x for x in fm.limit(1)]

        self.assertEqual(len(result), 1)

    def test_limit(self):
        fm = file_manager.FileManager([])

        fm.datasets = [
            mock.MagicMock(),
            mock.MagicMock(),
            mock.MagicMock(),
        ]

        result = [x for x in fm.limit(None)]

        self.assertEqual(len(result), 3)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_context_manager_error_opening(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
            cwt.Variable('file:///test3.nc', 'tas'),
        ]

        mock_file = mock.MagicMock()

        mock_open.side_effect = [
            mock_file, 
            cdms2.CDMSError('some error text'),
            mock.MagicMock(),
        ]

        with self.assertRaises(tasks.AccessError):
            with file_manager.FileManager(variables) as fm:
                pass

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_context_manager(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
            cwt.Variable('file:///test3.nc', 'tas'),
        ]

        with file_manager.FileManager(variables) as fm:
            self.assertEqual(len(fm.datasets), 1)

        mock_open.assert_called()
        self.assertEqual(mock_open.call_count, 3)

        mock_open.return_value.close.assert_called()
        self.assertEqual(mock_open.return_value.close.call_count, 3)
