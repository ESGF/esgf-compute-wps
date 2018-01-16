#! /usr/bin/env python

import cdms2
import cwt
import hashlib
import json
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

    def test_get_axis_error(self):
        axis_data = cdms2.createAxis([x for x in range(-90, 90)])

        axis_data.designateLatitude()

        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value.getAxisIndex.return_value = -1

        mock_file.__getitem__.return_value.getAxis.return_value = axis_data

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        with self.assertRaises(WPSError):
            axis = ds.get_axis('lat')

    def test_get_axis(self):
        axis_data = cdms2.createAxis([x for x in range(-90, 90)])

        axis_data.designateLatitude()

        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value.getAxisIndex.return_value = 0

        mock_file.__getitem__.return_value.getAxis.return_value = axis_data

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        axis = ds.get_axis('lat')

        self.assertEqual(axis_data, axis)

    def test_partitions_missing_axis(self):
        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        with self.assertRaises(WPSError):
            partitions = [chunk for chunk in ds.partitions('time')]

    def test_partitions_unknown(self):
        settings.PARITION_SIZE = 20 

        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value.getTime.return_value.mapInterval.return_value = (100, 200)

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal_axis = mock.MagicMock()

        ds.temporal_axis.id = 'time'

        ds.temporal_axis.isTime.return_value = True

        ds.temporal = None

        with self.assertRaises(WPSError):
            partitions = [chunk for chunk in ds.partitions('time')]

    def test_partitions_tuple(self):
        settings.PARITION_SIZE = 20 

        mock_file = mock.MagicMock()

        mock_file.return_value.__getitem__.return_value.getTime.return_value.mapInterval.return_value = (100, 200)

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal_axis = mock.MagicMock()

        ds.temporal_axis.id = 'time'

        ds.temporal_axis.isTime.return_value = True

        ds.temporal_axis.mapInterval.return_value = (100, 200)

        ds.temporal = (100, 200)

        expected = [(slice(x, x+settings.PARTITION_SIZE), {}) for x in xrange(100, 200, settings.PARTITION_SIZE)]

        partitions = [chunk for chunk in ds.partitions('time')]

        self.assertEqual(expected, partitions)

    def test_partitions_spatial(self):
        settings.PARITION_SIZE = 20 

        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal_axis = mock.MagicMock()

        ds.temporal_axis.id = 'time'

        ds.temporal_axis.isTime.return_value = True

        ds.temporal = slice(100, 200)

        ds.spatial = {'lat': slice(0, 90), 'lon': slice(0, 180)}

        # TODO finish

    def test_partitions_axis_from_file(self):
        settings.PARITION_SIZE = 20 

        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value.getAxisIndex.return_value = 0

        mock_axis = mock.MagicMock()

        mock_axis.isTime.return_value = True

        mock_file.__getitem__.return_value.getAxis.return_value = mock_axis

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal = slice(100, 200)

        ds.spatial = {}

        expected = [(slice(x, x+settings.PARTITION_SIZE), {}) for x in xrange(100, 200, settings.PARTITION_SIZE)]

        partitions = [chunk for chunk in ds.partitions('time')]

        self.assertEqual(expected, partitions)

    def test_partitions(self):
        settings.PARITION_SIZE = 20 

        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal_axis = mock.MagicMock()

        ds.temporal_axis.id = 'time'

        ds.temporal_axis.isTime.return_value = True

        ds.temporal = slice(100, 200)

        expected = [(slice(x, x+settings.PARTITION_SIZE), {}) for x in xrange(100, 200, settings.PARTITION_SIZE)]

        partitions = [chunk for chunk in ds.partitions('time')]

        self.assertEqual(expected, partitions)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists_temporal_mismatch(self, mock_open):
        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.latitude, helpers.longitude], 'tas')

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

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal = (100, 200)

        ds.spatial = {'lat': (24, 100), 'lon': (90, 180)}

        with self.assertNumQueries(2):
            ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNotNone(ds.cache)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists_spatial_mismatch(self, mock_open):
        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.latitude, helpers.longitude], 'tas')

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

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal = (100, 200)

        ds.spatial = {'lat': (24, 100), 'lon': (90, 180)}

        with self.assertNumQueries(2):
            ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNotNone(ds.cache)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists_cached_no_domain(self, mock_open):
        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.latitude, helpers.longitude], 'tas')

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

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

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
        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.latitude, helpers.longitude], 'tas')

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

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        with self.assertNumQueries(2):
            ds.check_cache()

        mock_open.assert_called_once()

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists_error(self, mock_open):
        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.latitude, helpers.longitude], 'tas')

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

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal = (100, 200)

        ds.spatial = {'lat': (24, 100), 'lon': (90, 180)}

        mock_open.side_effect = cdms2.CDMSError('some error text')

        ds.check_cache()

        self.assertEqual(ds.file_obj, mock_file)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_exists(self, mock_open):
        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.latitude, helpers.longitude], 'tas')

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

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal = (100, 200)

        ds.spatial = {'lat': (24, 100), 'lon': (90, 180)}

        with self.assertNumQueries(1):
            ds.check_cache()

        mock_open.assert_called_once()

        mock_file.close.assert_called_once()

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_error_opening(self, mock_open):
        mock_open.side_effect = cdms2.CDMSError('some error')

        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNone(ds.cache)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache_missing_temporal(self, mock_open):
        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.spatial = {
            'lat': slice(0, 90),
            'lon': slice(0, 180),
        }

        ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNotNone(ds.cache)
        self.assertEqual(ds.cache.dimensions, '{"temporal": null, "spatial": {"lat": {"slice": "0:90:None"}, "lon": {"slice": "0:180:None"}}}')

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_check_cache(self, mock_open):
        mock_file = mock.MagicMock()

        ds = file_manager.DataSet(mock_file, 'file:///test.nc', 'tas')

        ds.temporal = slice(100, 201)

        ds.spatial = {
            'lat': slice(0, 90),
            'lon': slice(0, 180),
        }

        with self.assertNumQueries(2):
            ds.check_cache()

        mock_open.assert_called_once()

        self.assertIsNotNone(ds.cache)
        self.assertEqual(ds.cache.dimensions, '{"temporal": {"slice": "100:201:None"}, "spatial": {"lat": {"slice": "0:90:None"}, "lon": {"slice": "0:180:None"}}}')

    def test_str_to_int_cannot_parse_float(self):
        ds = file_manager.DataSet(mock.MagicMock(), '', '')

        with self.assertRaises(WPSError):
            value = ds.str_to_int('5.5')

    def test_str_to_int(self):
        ds = file_manager.DataSet(mock.MagicMock(), '', '')

        value = ds.str_to_int('5')

        self.assertIsInstance(value, int)

    def test_str_to_int_float_cannot_parse(self):
        ds = file_manager.DataSet(mock.MagicMock(), '', '')

        with self.assertRaises(WPSError):
            value = ds.str_to_int_float('test')

    def test_str_to_int_float_to_float(self):
        ds = file_manager.DataSet(mock.MagicMock(), '', '')

        value = ds.str_to_int_float('5.0')

        self.assertIsInstance(value, float)

    def test_str_to_int_float(self):
        ds = file_manager.DataSet(mock.MagicMock(), '', '')

        value = ds.str_to_int_float('5')

        self.assertIsInstance(value, int)

    def test_dimension_to_cdms2_selector_crs_unknown(self):
        ds = file_manager.DataSet(mock.MagicMock(), '', '')

        dim = cwt.Dimension('time', 100, 200, cwt.CRS('unknown'))

        with self.assertRaises(WPSError):
            selector = ds.dimension_to_cdms2_selector(dim)

    def test_dimension_to_cdms2_selector_crs_indices(self):
        ds = file_manager.DataSet(mock.MagicMock(), '', '')

        dim = cwt.Dimension('time', 100, 200, cwt.INDICES)

        selector = ds.dimension_to_cdms2_selector(dim)

        self.assertEqual(selector, slice(100, 200, 1))

    def test_dimension_to_cdms2_selector(self):
        ds = file_manager.DataSet(mock.MagicMock(), '', '')

        dim = cwt.Dimension('time', 100, 200)

        selector = ds.dimension_to_cdms2_selector(dim)

        self.assertEqual(selector, (100, 200))

    def test_map_domain_missing_dimension(self):
        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.latitude, helpers.longitude], 'tas')

        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, 'file:///test1.nc', 'tas')

        domain = cwt.Domain([
            cwt.Dimension('time', '100', '200'),
            cwt.Dimension('lat', '0', '90'),
            cwt.Dimension('lon', '0', '180'),
            cwt.Dimension('does not exist', '10', '100'),
        ])

        with self.assertRaises(WPSError):
            ds.map_domain(domain)

    def test_map_domain(self):
        time = helpers.generate_time('days since 1990', 365)

        variable = helpers.generate_variable([time, helpers.latitude, helpers.longitude], 'tas')

        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value = variable

        ds = file_manager.DataSet(mock_file, 'file:///test1.nc', 'tas')

        domain = cwt.Domain([
            cwt.Dimension('time', '100', '200'),
            cwt.Dimension('lat', '0', '90'),
            cwt.Dimension('lon', '0', '180'),
        ])

        ds.map_domain(domain)

        self.assertEqual(ds.temporal, (100, 200))
        self.assertEqual(ds.spatial, {'lat': (0, 90), 'lon': (0, 180)})

    def test_get_time_error(self):
        mock_file_obj = mock.MagicMock()

        mock_file_obj.__getitem__.return_value.getTime.side_effect = cdms2.CDMSError('some error text')

        ds = file_manager.DataSet(mock_file_obj, 'file:///test.nc', 'tas')

        with self.assertRaises(tasks.AccessError):
            time = ds.get_time()

    def test_get_time(self):
        mock_file_obj = mock.MagicMock()

        ds = file_manager.DataSet(mock_file_obj, 'file:///test.nc', 'tas')

        time = ds.get_time()

        mock_file_obj.__getitem__.return_value.getTime.assert_called_once()

class FileManagerTestCase(test.TestCase):

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_sorted_limit(self, mock_open):
        filenames = ['file:///test1.nc', 'file:///test2.nc', 'file:///test3.nc']

        variables = [
            cwt.Variable(filenames[0], 'tas'),
            cwt.Variable(filenames[1], 'tas'),
            cwt.Variable(filenames[2], 'tas'),
        ]

        mock_open.return_value.__getitem__.return_value.getTime.side_effect = [
            mock.MagicMock(units='days since 1990'),
            mock.MagicMock(units='days since 1980'),
            mock.MagicMock(units='days since 2000'),
        ]

        with file_manager.FileManager(variables) as fm:
            values = [x.url for x in fm.sorted(1)]

            self.assertEqual(values, [filenames[1]])

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_sorted(self, mock_open):
        filenames = ['file:///test1.nc', 'file:///test2.nc', 'file:///test3.nc']

        variables = [
            cwt.Variable(filenames[0], 'tas'),
            cwt.Variable(filenames[1], 'tas'),
            cwt.Variable(filenames[2], 'tas'),
        ]

        mock_open.return_value.__getitem__.return_value.getTime.side_effect = [
            mock.MagicMock(units='days since 1990'),
            mock.MagicMock(units='days since 1980'),
            mock.MagicMock(units='days since 2000'),
        ]

        with file_manager.FileManager(variables) as fm:
            values = [x.url for x in fm.sorted()]

            self.assertEqual(values, [filenames[1], filenames[0], filenames[2]])

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

        mock_file.close.assert_called()
        self.assertEqual(mock_file.close.call_count, 1)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_context_manager(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
            cwt.Variable('file:///test3.nc', 'tas'),
        ]

        with file_manager.FileManager(variables) as fm:
            pass

        mock_open.assert_called()
        self.assertEqual(mock_open.call_count, 3)

        mock_open.return_value.close.assert_called()
        self.assertEqual(mock_open.return_value.close.call_count, 3)