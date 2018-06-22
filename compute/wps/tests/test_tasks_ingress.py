#! /usr/bin/env python

import cdms2
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
    def test_ingress_uri(self, mock_open):
        args = [
            'file:///test1.nc',
            'tas',
            self.domain1,
            'file:///test1_out.nc',
            0, 
            0,
        ]

        output = ingress.ingress_uri(*args)

        self.assertEqual(output, 'file:///test1_out.nc')
