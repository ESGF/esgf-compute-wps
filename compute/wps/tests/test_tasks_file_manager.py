#! /usr/bin/env python

import cdms2
import cwt
import mock
from django import test

from wps import tasks

class DataSetTestCase(test.TestCase):

    def test_get_time_error(self):
        mock_file_obj = mock.MagicMock()

        mock_file_obj.__getitem__.return_value.getTime.side_effect = cdms2.CDMSError('some error text')

        ds = tasks.DataSet(mock_file_obj, 'file:///test.nc', 'tas')

        with self.assertRaises(tasks.AccessError):
            time = ds.get_time()

    def test_get_time(self):
        mock_file_obj = mock.MagicMock()

        ds = tasks.DataSet(mock_file_obj, 'file:///test.nc', 'tas')

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

        fm  = tasks.FileManager.from_cwt_variables(variables)

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
            fm  = tasks.FileManager.from_cwt_variables(variables)

        mock_file_obj.close.assert_called_once()

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_from_cwt_variables_multiple_limit(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
        ]

        fm  = tasks.FileManager.from_cwt_variables(variables, 1)

        self.assertEqual(mock_open.call_count, 2)
        self.assertEqual(mock_open.return_value.close.call_count, 1)
        self.assertEqual(len(fm.datasets), 1)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_from_cwt_variables_multiple(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
            cwt.Variable('file:///test2.nc', 'tas'),
        ]

        fm  = tasks.FileManager.from_cwt_variables(variables)

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
            fm  = tasks.FileManager.from_cwt_variables(variables)

    @mock.patch('wps.tasks.file_manager.cdms2.open')
    def test_from_cwt_variables(self, mock_open):
        variables = [
            cwt.Variable('file:///test1.nc', 'tas'),
        ]

        fm  = tasks.FileManager.from_cwt_variables(variables)

        fm.close()

        self.assertEqual(mock_open.call_count, 1)
        self.assertEqual(mock_open.return_value.close.call_count, 1)
        self.assertEqual(len(fm.datasets), 1)

        dataset = fm.datasets[0]

        self.assertEqual(dataset.url, variables[0].uri)
        self.assertEqual(dataset.variable, variables[0].var_name)
