#! /usr/bin/env python

import mock

import cwt
from django import test

from wps import models
from wps.tasks import preprocess

class PreprocessTestCase(test.TestCase):
    fixtures = ['users.json',]

    def setUp(self):
        self.uris = [
            'file:///test1.nc',
            'file:///test2.nc',
            'file:///test3.nc',
        ]

        self.units = [
            'days since 2017-1-1 0',
            'days since 2018-1-1 0',
            'days since 2019-1-1 0',
        ]

        self.user = models.User.objects.first()

        self.domain1 = cwt.Domain(time=(0, 400), lat=(-90, 0), lon=(180, 360))

        self.domain2 = cwt.Domain(time=slice(0, 400), lat=(-90, 0), lon=(180, 360))

    def test_check_cache(self):
        data = preprocess.check_cache({})

        self.assertEqual(data, {'cached': None})

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('cdms2.open')
    def test_map_axis_indices(self, mock_load, mock_open):
        mock_open.side_effect = [mock.MagicMock() for x in self.uris]

        preprocess.get_uri = mock.MagicMock(side_effect=[x for x in self.uris])

        mock_time = mock.MagicMock()
        type(mock_time).id = mock.PropertyMock(return_value='time')
        type(mock_time).shape = mock.PropertyMock(side_effect=[(122,),(120,),(120,)])

        mock_lat = mock.MagicMock()
        type(mock_lat).id = mock.PropertyMock(return_value='lat')
        type(mock_lat).shape = mock.PropertyMock(return_value=(100,))
        mock_lat.mapInterval.return_value = (0, 100)

        mock_lon = mock.MagicMock()
        type(mock_lon).id = mock.PropertyMock(return_value='lon')
        type(mock_lon).shape = mock.PropertyMock(return_value=(200,))
        mock_lon.mapInterval.return_value = (0, 200)

        get_axis_side_effect = [
            mock_time,
            mock_time,
            mock_time,
        ]

        preprocess.get_axis = mock.MagicMock(side_effect=get_axis_side_effect)

        preprocess.get_axis_list = mock.MagicMock(return_value=[mock_lat, mock_lon])

        expected = {
            'var_name': 'tas',
            'axis': 'time',
            'axis_slice': [0, 400],
            'axis_map': {
                self.uris[0]: {
                    'time': slice(0, 122),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
                self.uris[1]: {
                    'time': slice(0, 120),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
                self.uris[2]: {
                    'time': slice(0, 120),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
            }
        }

        data = preprocess.map_axis_indices(self.uris, 'tas', 'time', self.domain2, self.user.id)

        self.assertEqual(data, expected)

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('wps.tasks.preprocess.get_axis')
    @mock.patch('cdms2.open')
    def test_map_axis_values(self, mock_load, mock_get, mock_open):
        mock_time = mock.MagicMock()
        mock_time.clone.return_value.id = 'time'
        mock_time.clone.return_value.mapInterval.return_value = [0, 400]

        mock_time2 = mock.MagicMock()
        mock_time2.id = 'time'
        mock_time2.mapInterval.return_value = [0, 100]

        mock_lat = mock.MagicMock()
        mock_lat.id = 'lat'
        mock_lat.mapInterval.return_value = [0, 100]

        mock_lon = mock.MagicMock()
        mock_lon.id = 'lon'
        mock_lon.mapInterval.return_value = [0, 200]

        mock_get.side_effect = [
            mock_time,
            mock_time2,
            mock_lat,
            mock_lon,
        ]

        expected = {
            'var_name': 'tas',
            'axis': 'time',
            'axis_slice': [0, 400],
            'uri': self.uris[0],
            'axis_map': {
                'time': slice(0, 400),
                'lat': slice(0, 100),
                'lon': slice(0, 200),
            }
        }

        data = preprocess.map_axis_values(self.units[0], self.uris[0], 'tas', 'time', self.domain1, self.user.id)

        self.assertEqual(data, expected)

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('wps.tasks.preprocess.get_axis')
    @mock.patch('cdms2.open')
    def test_determine_base_units_same(self, mock_load, mock_get, mock_open):
        mock_get.return_value.units = self.units[0]

        check = preprocess.determine_base_units(self.uris, 'tas', 'time', self.user.id)

        self.assertEqual(check, self.units[0])

        self.assertEqual(mock_get.call_count, 3)

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('wps.tasks.preprocess.get_axis')
    @mock.patch('cdms2.open')
    def test_determine_base_units(self, mock_load, mock_get, mock_open):
        mock_get.side_effect = [
            mock.MagicMock(units=self.units[0]),
            mock.MagicMock(units=self.units[2]),
            mock.MagicMock(units=self.units[1]),
        ]

        check = preprocess.determine_base_units(self.uris, 'tas', 'time', self.user.id)

        self.assertEqual(check, self.units[0])

        self.assertEqual(mock_get.call_count, 3)
