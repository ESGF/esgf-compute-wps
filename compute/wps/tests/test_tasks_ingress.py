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

    @mock.patch('cdms2.open')
    def test_ingress_uri_outfile_error(self, mock_open):
        mock_open.side_effect = [
            mock.MagicMock(),
            cdms2.CDMSError(),
        ]

        args = [
            'file:///test1.nc',
            'tas',
            self.domain1,
            'file:///test1_out.nc',
            0, 
            0,
        ]

        with self.assertRaises(WPSError):
            output = ingress.ingress_uri(*args)

    @mock.patch('cdms2.open')
    def test_ingress_uri_infile_error(self, mock_open):
        mock_open.side_effect = cdms2.CDMSError()

        args = [
            'file:///test1.nc',
            'tas',
            self.domain1,
            'file:///test1_out.nc',
            0, 
            0,
        ]

        with self.assertRaises(WPSError):
            output = ingress.ingress_uri(*args)

    @mock.patch('cdms2.open')
    @mock.patch('os.stat')
    @mock.patch('wps.tasks.ingress.get_now')
    def test_ingress_uri_no_time(self, mock_get, mock_stat, mock_open):
        type(mock_stat.return_value).st_size = mock.PropertyMock(return_value=3222111)

        args = [
            'file:///test1.nc',
            'tas',
            self.domain2,
            'file:///test1_out.nc',
            0, 
            0,
        ]

        start = datetime.datetime(2016, 6, 12, second=32)

        stop = datetime.datetime(2016, 6, 12, second=55)

        mock_get.side_effect = [
            start,
            stop,
        ]

        output = ingress.ingress_uri(*args)

        expected = {
            'file:///test1.nc': {
                'local': 'file:///test1_out.nc',
                'elapsed': stop - start,
                'size': 3.222111,
            },
        }

        self.assertEqual(output, expected)

    @mock.patch('cdms2.open')
    @mock.patch('os.stat')
    @mock.patch('wps.tasks.ingress.get_now')
    def test_ingress_uri(self, mock_get, mock_stat, mock_open):
        type(mock_stat.return_value).st_size = mock.PropertyMock(return_value=3222111)

        args = [
            'file:///test1.nc',
            'tas',
            self.domain1,
            'file:///test1_out.nc',
            0, 
            0,
        ]

        start = datetime.datetime(2016, 6, 12, second=32)

        stop = datetime.datetime(2016, 6, 12, second=55)

        mock_get.side_effect = [
            start,
            stop,
        ]

        output = ingress.ingress_uri(*args)

        expected = {
            'file:///test1.nc': {
                'local': 'file:///test1_out.nc',
                'elapsed': stop - start,
                'size': 3.222111,
            },
        }

        self.assertEqual(output, expected)
