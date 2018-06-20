#! /usr/bin/env python

import json
import mock

import cwt
import requests
from django import test
from django.conf import settings

import wps
from wps import helpers
from wps import models
from wps.tasks import preprocess

class PreprocessTestCase(test.TestCase):

    fixtures = ['users.json']

    def setUp(self):
        self.user = models.User.objects.first()

        self.uris = [
            'file:///test1.nc',
            'file:///test2.nc',
            'file:///test3.nc',
        ]

        self.mock_time = mock.MagicMock()
        type(self.mock_time).id = mock.PropertyMock(return_value='time')
        type(self.mock_time.getTime.return_value).units = mock.PropertyMock(return_value='days since 1990-1-1 0')
        type(self.mock_time.clone.return_value).id = mock.PropertyMock(return_value='time')
        self.mock_time.isTime.return_value = True
        self.mock_time.clone.return_value.mapInterval.return_value = (0, 122)

        self.mock_time2 = mock.MagicMock()
        type(self.mock_time2).id = mock.PropertyMock(return_value='time')
        type(self.mock_time2.getTime.return_value).units = mock.PropertyMock(return_value='days since 2000-1-1 0')

        self.mock_time3 = mock.MagicMock()
        type(self.mock_time3).id = mock.PropertyMock(return_value='time')
        type(self.mock_time3.getTime.return_value).units = mock.PropertyMock(return_value='days since 2010-1-1 0')

        self.mock_lat = mock.MagicMock()
        type(self.mock_lat).id = mock.PropertyMock(return_value='lat')
        self.mock_lat.isTime.return_value = False
        self.mock_lat.mapInterval.return_value = (0, 100)

        self.mock_lon = mock.MagicMock()
        type(self.mock_lon).id = mock.PropertyMock(return_value='lon')
        self.mock_lon.isTime.return_value = False
        self.mock_lon.mapInterval.return_value = (0, 200)

        self.domain1 = cwt.Domain(time=(0, 200), lat=(-90, 0), lon=(180, 360))
        self.domain2 = cwt.Domain(time=(0, 400), lat=(-90, 0), lon=(180, 360))

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('cdms2.open')
    @mock.patch('wps.tasks.preprocess.get_axis_list')
    def test_map_domain_map_interval_failed(self, mock_axis, mock_open, mock_load):
        self.mock_lon.mapInterval.side_effect = TypeError()

        mock_axis.return_value = [
            self.mock_time,
            self.mock_lat,
            self.mock_lon,
        ]
        
        attrs = {
            'base_units': 'days since 1990-1-1 0',
        }

        data = preprocess.map_domain(attrs, self.uris[0], 'tas', self.domain1, self.user.id)

        expected = attrs.copy()

        expected['domain'] = {
            self.uris[0]: {
                'time': (0, 200),
                'lat': (-90, 0),
                'lon': (180, 360),
            }
        }

        expected['mapped'] = {
            self.uris[0]: None,
        }

        self.assertEqual(data, expected)

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('cdms2.open')
    @mock.patch('wps.tasks.preprocess.get_axis_list')
    def test_map_domain(self, mock_axis, mock_open, mock_load):
        mock_axis.return_value = [
            self.mock_time,
            self.mock_lat,
            self.mock_lon,
        ]
        
        attrs = {
            'base_units': 'days since 1990-1-1 0',
            self.uris[0]: {},
        }

        data = preprocess.map_domain(attrs, self.uris[0], 'tas', self.domain1, self.user.id)

        expected = attrs.copy()

        expected['domain'] = {
            'time': (0, 200),
            'lat': (-90, 0),
            'lon': (180, 360),
        }

        expected[self.uris[0]] = {
            'mapped': {
                'time': slice(0, 122),
                'lat': slice(0, 100),
                'lon': slice(0, 200),
            }
        }

        self.assertEqual(data, expected)

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('cdms2.open')
    @mock.patch('wps.tasks.preprocess.get_variable')
    def test_determine_base_units_no_files(self, mock_variable, mock_open, mock_load):
        with self.assertRaises(wps.WPSError):
            data = preprocess.determine_base_units([], 'tas', self.user.id)

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('cdms2.open')
    @mock.patch('wps.tasks.preprocess.get_variable')
    def test_determine_base_units(self, mock_variable, mock_open, mock_load):
        mock_variable.side_effect = [
            self.mock_time3,
            self.mock_time,
            self.mock_time2,
        ]

        data = preprocess.determine_base_units(self.uris, 'tas', self.user.id)

        expected = {
            'base_units': 'days since 1990-1-1 0',
        }

        self.assertEqual(data, expected)

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('wps.models.User.objects.get')
    def test_load_credentials_missing_user(self, mock_get, mock_load):
        mock_get.side_effect = models.User.DoesNotExist()

        with self.assertRaises(wps.WPSError):
            preprocess.load_credentials(self.user.id)

    @mock.patch('wps.tasks.credentials.load_certificate')
    def test_load_credentials(self, mock_load):
        preprocess.load_credentials(self.user.id)

        mock_load.assert_called_with(self.user)
