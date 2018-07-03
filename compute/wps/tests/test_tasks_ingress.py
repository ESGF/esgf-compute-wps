#! /usr/bin/env python

import cdms2
import datetime
import mock
from django import test

from wps import WPSError
from wps.tasks import ingress

class PreprocessTestCase(test.TestCase):
    def setUp(self):
        self.domain1 = {
            'time': slice(0, 10),
            'lat': slice(0, 100),
            'lon': slice(0, 200),
        }

        self.start1 = datetime.datetime(2017, 10, 23, second=10)
        self.stop1 = datetime.datetime(2017, 10, 23, second=30)
        self.elapsed1 = self.stop1 - self.start1

        self.args1 = [
            'key1',
            'file:///test1.nc',
            'tas',
            self.domain1,
            'file:///test1_ingress.nc', 
            0,
            0,
        ]

        self.ingress1 = {
            'key1': {
                'ingress': {
                    'elapsed': self.elapsed1,
                    'size': 1.2,
                    'path': 'file:///test1_ingress.nc',
                    'uri': 'file:///test1.nc',
                }
            }
        }

        self.ingress2 = {
            'key2': {
                'ingress': {
                    'elapsed': self.elapsed1,
                    'size': 1.2,
                    'path': 'file:///test2_ingress.nc',
                    'uri': 'file:///test1.nc',
                }
            }
        }

        self.cache1 = self.ingress1.copy()

        self.cache1.update(self.ingress2)

    @mock.patch('os.remove')
    def test_ingress_cleanup_remove_error(self, mock_remove):
        mock_remove.side_effect = OSError()

        data = ingress.ingress_cleanup(self.cache1, job_id=0)

        self.assertEqual(mock_remove.call_count, 2)

    @mock.patch('os.remove')
    def test_ingress_cleanup(self, mock_remove):
        data = ingress.ingress_cleanup(self.cache1, job_id=0)

        self.assertEqual(mock_remove.call_count, 2)

    @mock.patch('wps.tasks.preprocess.check_cache_entries')
    @mock.patch('cdms2.open')
    @mock.patch('wps.models.Cache.objects.create')
    def test_ingress_cache_multiple_inputs(self, mock_create, mock_open, mock_cache):
        mock_cache.return_value = None

        args = [
            [self.ingress1, self.ingress2],
            'file:///test1.nc',
            'tas',
            self.domain1,
            'days since 1990-1-1 0',
        ]

        data = ingress.ingress_cache(*args, job_id=0)

        self.assertEqual(data, self.cache1)

        mock_cache.assert_called_with('file:///test1.nc', 'tas', self.domain1)

        mock_create.assert_called_once()

        self.assertEqual(mock_open.call_count, 3)

    @mock.patch('wps.tasks.preprocess.check_cache_entries')
    @mock.patch('cdms2.open')
    @mock.patch('wps.models.Cache.objects.create')
    def test_ingress_cache_output_error(self, mock_create, mock_open, mock_cache):
        mock_cache.return_value = None

        mock_open.side_effect = cdms2.CDMSError()

        args = [
            [self.ingress1,],
            'file:///test1.nc',
            'tas',
            self.domain1,
            'days since 1990-1-1 0',
        ]

        with self.assertRaises(WPSError):
            data = ingress.ingress_cache(*args, job_id=0)

    @mock.patch('wps.tasks.preprocess.check_cache_entries')
    @mock.patch('cdms2.open')
    @mock.patch('wps.models.Cache.objects.create')
    def test_ingress_cache_input_error(self, mock_create, mock_open, mock_cache):
        mock_cache.return_value = None

        mock_open.side_effect = [
            mock.MagicMock(),
            cdms2.CDMSError(),
        ]

        args = [
            [self.ingress1,],
            'file:///test1.nc',
            'tas',
            self.domain1,
            'days since 1990-1-1 0',
        ]

        with self.assertRaises(WPSError):
            data = ingress.ingress_cache(*args, job_id=0)

    @mock.patch('wps.tasks.preprocess.check_cache_entries')
    @mock.patch('cdms2.open')
    @mock.patch('wps.models.Cache.objects.create')
    def test_ingress_cache(self, mock_create, mock_open, mock_cache):
        mock_cache.return_value = None

        args = [
            [self.ingress1,],
            'file:///test1.nc',
            'tas',
            self.domain1,
            'days since 1990-1-1 0',
        ]

        data = ingress.ingress_cache(*args, job_id=0)

        mock_create.return_value.set_size.assert_called()

        mock_cache.assert_called_with('file:///test1.nc', 'tas', self.domain1)

        mock_create.assert_called_once()

        self.assertEqual(mock_open.call_count, 2)

        self.assertEqual(data, self.ingress1)
        
    @mock.patch('wps.tasks.preprocess.load_credentials')
    @mock.patch('wps.tasks.ingress.get_now')
    @mock.patch('cdms2.open')
    @mock.patch('wps.tasks.ingress.read_data')
    @mock.patch('os.stat')
    def test_ingress_uri_output_error(self, mock_stat, mock_read, mock_open, mock_now, mock_load):
        type(mock_stat.return_value).st_size = 1200000

        mock_now.side_effect = [
            self.start1,
            self.stop1,
        ]

        mock_open.side_effect = [
            mock.MagicMock(),
            cdms2.CDMSError()
        ]

        with self.assertRaises(WPSError):
            data = ingress.ingress_uri(*self.args1)

    @mock.patch('wps.tasks.preprocess.load_credentials')
    @mock.patch('wps.tasks.ingress.get_now')
    @mock.patch('cdms2.open')
    @mock.patch('wps.tasks.ingress.read_data')
    @mock.patch('os.stat')
    def test_ingress_uri_input_error(self, mock_stat, mock_read, mock_open, mock_now, mock_load):
        type(mock_stat.return_value).st_size = 1200000

        mock_now.side_effect = [
            self.start1,
            self.stop1,
        ]

        mock_open.side_effect = cdms2.CDMSError()

        with self.assertRaises(WPSError):
            data = ingress.ingress_uri(*self.args1)

    @mock.patch('wps.tasks.preprocess.load_credentials')
    @mock.patch('wps.tasks.ingress.get_now')
    @mock.patch('cdms2.open')
    @mock.patch('wps.tasks.ingress.read_data')
    @mock.patch('os.stat')
    def test_ingress_uri(self, mock_stat, mock_read, mock_open, mock_now, mock_load):
        type(mock_stat.return_value).st_size = 1200000

        mock_now.side_effect = [
            self.start1,
            self.stop1,
        ]

        data = ingress.ingress_uri(*self.args1)

        self.assertEqual(data, self.ingress1)

        self.assertEqual(mock_open.call_count, 2)
        self.assertEqual(mock_read.call_count, 1)
