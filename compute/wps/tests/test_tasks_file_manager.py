#! /usr/bin/env python

import cdms2
import cwt
import mock
from django import test

from . import helpers
from wps import tasks
from wps.tasks import file_manager
from wps import WPSError

class DataSetTestCase(test.TestCase):

    def setUp(self):
        self.time = helpers.generate_time('days since 1990', 365)

        self.variable = helpers.generate_variable([self.time, helpers.latitude, helpers.longitude], 'tas')

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
        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value = self.variable

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
        mock_file = mock.MagicMock()

        mock_file.__getitem__.return_value = self.variable

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
    def test_from_cwt_variables_multiple_ordering(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
        ]

        mock_file1 = mock.MagicMock()
        mock_file1.__getitem__.return_value.getTime.return_value.units = '2'

        mock_file2 = mock.MagicMock()
        mock_file2.__getitem__.return_value.getTime.return_value.units = '1'

        mock_open.side_effect = [mock_file1, mock_file2]

        fm  = file_manager.FileManager.from_cwt_variables(variables)

        self.assertEqual(len(fm.datasets), 2)

        self.assertEqual(fm.datasets[0].url, 'file:///test2.nc')
        self.assertEqual(fm.datasets[1].url, 'file:///test1.nc')

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_from_cwt_variables_multiple_cleanup(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
        ]

        mock_file_obj = mock.MagicMock()

        mock_open.side_effect = [mock_file_obj, cdms2.CDMSError]

        with self.assertRaises(tasks.AccessError):
            fm  = file_manager.FileManager.from_cwt_variables(variables)

        mock_file_obj.close.assert_called_once()

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_from_cwt_variables_multiple_limit(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
        ]

        fm  = file_manager.FileManager.from_cwt_variables(variables, 1)

        self.assertEqual(mock_open.call_count, 2)
        self.assertEqual(mock_open.return_value.close.call_count, 1)
        self.assertEqual(len(fm.datasets), 1)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_from_cwt_variables_multiple(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
        ]

        fm  = file_manager.FileManager.from_cwt_variables(variables)

        fm.close()

        self.assertEqual(mock_open.call_count, 2)
        self.assertEqual(mock_open.return_value.close.call_count, 2)
        self.assertEqual(len(fm.datasets), 2)
        
    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_from_cwt_variables_open_error(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
        ]

        mock_open.side_effect = cdms2.CDMSError('some error text')

        with self.assertRaises(tasks.AccessError):
            fm  = file_manager.FileManager.from_cwt_variables(variables)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_from_cwt_variables(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
        ]

        fm  = file_manager.FileManager.from_cwt_variables(variables)

        fm.close()

        self.assertEqual(mock_open.call_count, 1)
        self.assertEqual(mock_open.return_value.close.call_count, 1)
        self.assertEqual(len(fm.datasets), 1)

        dataset = fm.datasets[0]

        self.assertEqual(dataset.url, variables[0].uri)
        self.assertEqual(dataset.variable_name, variables[0].var_name)
