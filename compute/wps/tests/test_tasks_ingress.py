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
            'time': slice(0, 200),
            'lat': (-90, 0),
            'lon': (180, 360),
        }

        self.domain2 = {
            'lat': (-90, 0),
            'lon': (180, 360),
        }

        self.args = [
            'file:///test1.nc',
            'tas',
            self.domain1,
            'file:///test1_out.nc',
            0, 
            0,
        ]

        self.args2 = [
            'file:///test1.nc',
            'tas',
            self.domain2,
            'file:///test1_out.nc',
            0, 
            0,
        ]

        self.ingress_cache_attrs = {
            'file:///test1.nc': {
                'file:///test1_01.nc': {
                    'elapsed': datetime.timedelta(seconds=3),
                    'size': 1.2,
                },
                'file:///test1_02.nc': {
                    'elapsed': datetime.timedelta(seconds=1),
                    'size': 1.2,
                },
            },
            'file:///test2.nc': {
                'file:///test1_03.nc': {
                    'elapsed': datetime.timedelta(seconds=2),
                    'size': 1.2,
                },
            },
        }

        self.ingress_cache_mapped = {
            'time': slice(0, 100),
            'lat': slice(-90, 0),
            'lon': slice(100, 200),
        }

    def test_ingress_cache(self):
        output = ingress.ingress_cache(self.ingress_cache_attrs, 'file:///test1.nc', 'tas', self.ingress_cache_mapped)

        self.assertEqual(output, self.ingress_cache_attrs)

    @mock.patch('cdms2.open')
    def test_ingress_uri_outfile_error(self, mock_open):
        mock_open.side_effect = [
            mock.MagicMock(),
            cdms2.CDMSError(),
        ]

        with self.assertRaises(WPSError):
            output = ingress.ingress_uri(*self.args)

    @mock.patch('cdms2.open')
    def test_ingress_uri_infile_error(self, mock_open):
        mock_open.side_effect = cdms2.CDMSError()

        with self.assertRaises(WPSError):
            output = ingress.ingress_uri(*self.args)

    @mock.patch('cdms2.open')
    @mock.patch('os.stat')
    @mock.patch('wps.tasks.ingress.get_now')
    def test_ingress_uri_no_time(self, mock_get, mock_stat, mock_open):
        type(mock_stat.return_value).st_size = mock.PropertyMock(return_value=3222111)

        start = datetime.datetime(2016, 6, 12, second=32)

        stop = datetime.datetime(2016, 6, 12, second=55)

        mock_get.side_effect = [
            start,
            stop,
        ]

        output = ingress.ingress_uri(*self.args2)

        expected = {
            'file:///test1.nc': {
                'file:///test1_out.nc': {
                    'elapsed': stop - start,
                    'size': 3.222111,
                },
            },
        }

        self.assertEqual(output, expected)

    @mock.patch('cdms2.open')
    @mock.patch('os.stat')
    @mock.patch('wps.tasks.ingress.get_now')
    def test_ingress_uri(self, mock_get, mock_stat, mock_open):
        type(mock_stat.return_value).st_size = mock.PropertyMock(return_value=3222111)

        start = datetime.datetime(2016, 6, 12, second=32)

        stop = datetime.datetime(2016, 6, 12, second=55)

        mock_get.side_effect = [
            start,
            stop,
        ]

        output = ingress.ingress_uri(*self.args)

        expected = {
            'file:///test1.nc': {
                'file:///test1_out.nc': {
                    'elapsed': stop - start,
                    'size': 3.222111,
                },
            },
        }

        self.assertEqual(output, expected)
