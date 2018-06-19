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

class MockFilter:
    def __init__(self, items):
        self.items = items

    def count(self):
        return len(self.items)

    def __len__(self):
        return len(self.items)

    def __getitem__(self, x):
        return self.items[x]

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

        self.domain3 = cwt.Domain([cwt.Dimension('level', 0, 200, cwt.CRS('some_new'))])

        self.collect_execute1 = [
            {
                'var_name': 'tas',
                'axis': 'time',
                'axis_slice': [0, 400],
                'axis_map': {
                    self.uris[0]: {
                        'time': slice(60, 122),
                        'lat': slice(0, 100),
                        'lon': slice(0, 200),
                    },
                },
                'cached': {
                    self.uris[0]: 'file:///test1.nc',
                },
                'chunks': {
                    self.uris[0]: {
                        'time': [
                            slice(60, 70), 
                            slice(70, 80), 
                            slice(80, 90), 
                            slice(90, 100), 
                            slice(100, 122), 
                        ],
                        'lat': slice(0, 100),
                        'lon': slice(0, 200),
                    },
                },
            },
            {
                'var_name': 'tas',
                'axis': 'time',
                'axis_slice': [0, 400],
                'axis_map': {
                    self.uris[1]: {
                        'time': slice(0, 60),
                        'lat': slice(0, 100),
                        'lon': slice(0, 200),
                    },
                },
                'cached': {
                    self.uris[1]: 'file:///test2.nc',
                },
                'chunks': {
                    self.uris[1]: {
                        'time': [
                            slice(0, 10),
                            slice(10, 20),
                            slice(20, 30),
                            slice(30, 40),
                            slice(40, 50),
                            slice(50, 60),
                        ],
                        'lat': slice(0, 100),
                        'lon': slice(0, 200),
                    },
                },
            },
            {
                'var_name': 'tas',
                'axis': 'time',
                'axis_slice': [0, 400],
                'axis_map': {
                    self.uris[2]: {
                        'time': slice(0, 120),
                        'lat': slice(0, 100),
                        'lon': slice(0, 200),
                    },
                },
                'cached': {
                    self.uris[2]: None,
                },
                'chunks': {
                    self.uris[2]: None,
                },
            },
        ]

    @mock.patch('requests.post')
    def test_collect_and_execute_http_error(self, mock_post):
        mock_post.side_effect = requests.HTTPError('HTTP error')

        with self.assertRaises(wps.WPSError):
            preprocess.collect_and_execute(self.collect_execute1)

    @mock.patch('requests.post')
    def test_collect_and_execute_connection_error(self, mock_post):
        mock_post.side_effect = requests.ConnectionError('Connection error')

        with self.assertRaises(wps.WPSError):
            preprocess.collect_and_execute(self.collect_execute1)

    @mock.patch('requests.post')
    def test_collect_and_execute(self, mock_post):
        preprocess.collect_and_execute(self.collect_execute1)

        expected = {
            'var_name': 'tas',
            'axis': 'time',
            'axis_slice': [0, 400],
            'axis_map': {
                self.uris[0]: {
                    'time': slice(60, 122),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
                self.uris[1]: {
                    'time': slice(0, 60),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
                self.uris[2]: {
                    'time': slice(0, 120),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
            },
            'cached': {
                self.uris[0]: 'file:///test1.nc',
                self.uris[1]: 'file:///test2.nc',
                self.uris[2]: None,
            },
            'chunks': {
                self.uris[0]: {
                    'time': [
                        slice(60, 70), 
                        slice(70, 80), 
                        slice(80, 90), 
                        slice(90, 100), 
                        slice(100, 122), 
                    ],
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
                self.uris[1]: {
                    'time': [
                        slice(0, 10),
                        slice(10, 20),
                        slice(20, 30),
                        slice(30, 40),
                        slice(40, 50),
                        slice(50, 60),
                    ],
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
                self.uris[2]: None,
            },
        }

        expected_data = json.dumps(expected, default=helpers.json_dumps_default)

        mock_post.assert_called_once_with(settings.WPS_EXECUTE_URL, data=expected_data, verify=False) 

    def test_generate_chunks_axis_mapped_none(self):
        data = {}

        data = preprocess.generate_chunks(data, self.uris[0], 'time')

        expected = { 'chunks': None }

        self.assertEqual(data, expected)

    def test_generate_chunks(self):
        self.maxDiff = None
        data = {
            'axis_map': {
                self.uris[0]: {
                    'time': slice(0, 122),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                }
            },
        }

        data = preprocess.generate_chunks(data, self.uris[0], 'time')

        expected = { 
            'axis_map': {
                self.uris[0]: {
                    'time': slice(0, 122),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                }
            },
            'chunks': {
                self.uris[0]: {
                    'time': [
                        slice(0, 10),
                        slice(10, 20),
                        slice(20, 30),
                        slice(30, 40),
                        slice(40, 50),
                        slice(50, 60),
                        slice(60, 70),
                        slice(70, 80),
                        slice(80, 90),
                        slice(90, 100),
                        slice(100, 110),
                        slice(110, 120),
                        slice(120, 122),
                    ],
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                }
            }
        }

        self.assertEqual(data, expected)

    def test_map_remaining_axes_not_in_domain(self):
        mock_time = mock.MagicMock()
        type(mock_time).id = mock.PropertyMock(return_value='time')

        mock_lat = mock.MagicMock()
        type(mock_lat).id = mock.PropertyMock(return_value='lat')
        mock_lat.mapInterval.return_value = [0, 100]

        mock_lon = mock.MagicMock()
        type(mock_lon).id = mock.PropertyMock(return_value='lon')
        mock_lon.mapInterval.return_value = [0, 200]

        mock_level = mock.MagicMock()
        type(mock_level).id = mock.PropertyMock(return_value='level')
        type(mock_level).shape = mock.PropertyMock(return_value=(200,))
        mock_level.mapInterval.return_value = [0, 200]

        mock_file = mock.MagicMock()

        preprocess.get_axis_list = mock.MagicMock(return_value=[mock_level, mock_lat, mock_lon])

        data = preprocess.map_remaining_axes(mock_file, 'tas', self.domain2, ['time'])

        expected = {
            'lat': slice(0, 100),
            'lon': slice(0, 200),
            'level': slice(0, 200),
        }

        self.assertEqual(data, expected)

    def test_map_remaining_axes_map_interval_error(self):
        mock_time = mock.MagicMock()
        type(mock_time).id = mock.PropertyMock(return_value='time')

        mock_lat = mock.MagicMock()
        type(mock_lat).id = mock.PropertyMock(return_value='lat')
        mock_lat.mapInterval.return_value = [0, 100]

        mock_lon = mock.MagicMock()
        type(mock_lon).id = mock.PropertyMock(return_value='lon')
        mock_lon.mapInterval.side_effect = TypeError()

        mock_file = mock.MagicMock()

        preprocess.get_axis_list = mock.MagicMock(return_value=[mock_lat, mock_lon])

        with self.assertRaises(wps.WPSError):
            data = preprocess.map_remaining_axes(mock_file, 'tas', self.domain2, ['time'])

    def test_map_remaining_axes_unknwon_crs(self):
        mock_level = mock.MagicMock()
        type(mock_level).id = mock.PropertyMock(return_value='level')

        mock_file = mock.MagicMock()

        preprocess.get_axis_list = mock.MagicMock(return_value=[mock_level])

        with self.assertRaises(wps.WPSError):
            data = preprocess.map_remaining_axes(mock_file, 'tas', self.domain3, ['time'])

    def test_map_remaining_axes(self):
        mock_time = mock.MagicMock()
        type(mock_time).id = mock.PropertyMock(return_value='time')

        mock_lat = mock.MagicMock()
        type(mock_lat).id = mock.PropertyMock(return_value='lat')
        mock_lat.mapInterval.return_value = [0, 100]

        mock_lon = mock.MagicMock()
        type(mock_lon).id = mock.PropertyMock(return_value='lon')
        mock_lon.mapInterval.return_value = [0, 200]

        mock_file = mock.MagicMock()

        preprocess.get_axis_list = mock.MagicMock(return_value=[mock_lat, mock_lon])

        data = preprocess.map_remaining_axes(mock_file, 'tas', self.domain2, ['time'])

        expected = {
            'lat': slice(0, 100),
            'lon': slice(0, 200),
        }

        self.assertEqual(data, expected)

    def test_get_axis_list_excluded(self):
        mock_time = mock.MagicMock()
        type(mock_time).id = mock.PropertyMock(return_value='time')

        mock_lat = mock.MagicMock()
        type(mock_lat).id = mock.PropertyMock(return_value='lat')

        mock_lon = mock.MagicMock()
        type(mock_lon).id = mock.PropertyMock(return_value='lon')

        mock_file = mock.MagicMock()
        mock_file.__getitem__.return_value.getAxisList.return_value = [
            mock_time,
            mock_lat,
            mock_lon,
        ]

        data = preprocess.get_axis_list(mock_file, 'tas', ['time'])

        expected = [mock_lat, mock_lon]

        self.assertEqual(data, expected)

    def test_get_axis_list(self):
        mock_time = mock.MagicMock()
        type(mock_time).id = mock.PropertyMock(return_value='time')

        mock_lat = mock.MagicMock()
        type(mock_lat).id = mock.PropertyMock(return_value='lat')

        mock_lon = mock.MagicMock()
        type(mock_lon).id = mock.PropertyMock(return_value='lon')

        mock_file = mock.MagicMock()
        mock_file.__getitem__.return_value.getAxisList.return_value = [
            mock_time,
            mock_lat,
            mock_lon,
        ]

        data = preprocess.get_axis_list(mock_file, 'tas', [])

        expected = [mock_time, mock_lat, mock_lon]

        self.assertEqual(data, expected)

    def test_get_axis_missing(self):
        mock_axis = mock.MagicMock()

        mock_file = mock.MagicMock()
        mock_file.__getitem__.return_value.getAxisIndex.return_value = -1

        with self.assertRaises(wps.WPSError):
            data = preprocess.get_axis(mock_file, 'tas', 'time')

    def test_get_axis(self):
        mock_axis = mock.MagicMock()

        mock_file = mock.MagicMock()
        mock_file.__getitem__.return_value.getAxis.return_value = mock_axis

        data = preprocess.get_axis(mock_file, 'tas', 'time')

        self.assertEqual(data, mock_axis)

    def test_get_uri(self):
        mock_file = mock.MagicMock()
        type(mock_file).uri = mock.PropertyMock(return_value='file:///test.nc')

        data = preprocess.get_uri(mock_file)

        self.assertEqual(data, 'file:///test.nc')

    def test_get_axis_slice(self):
        data = preprocess.get_axis_slice(self.domain1, 'time')

        self.assertEqual(data, [0, 400])

    @mock.patch('wps.models.Cache.objects.filter')
    def test_check_cache_not_superset(self, mock_filter):
        mock_cached1 = mock.MagicMock()
        type(mock_cached1).url = mock.PropertyMock(return_value='file:///test_cached.nc')
        type(mock_cached1).valid = mock.PropertyMock(return_value=True)
        mock_cached1.is_superset.return_value = False

        mock_filter.return_value = MockFilter([mock_cached1])

        prev = {
            'var_name': 'tas',
            'axis_map': {
                self.uris[0]: {
                    'time': slice(0, 122),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
            },
        }

        expected = prev.copy()

        expected['cached'] = {
            self.uris[0]: None,
        }

        data = preprocess.check_cache(prev, self.uris[0])

        self.assertEqual(data, expected)

    @mock.patch('wps.models.Cache.objects.filter')
    def test_check_cache_invalid(self, mock_filter):
        mock_cached1 = mock.MagicMock()
        type(mock_cached1).url = mock.PropertyMock(return_value='file:///test_cached.nc')
        type(mock_cached1).valid = mock.PropertyMock(return_value=False)

        mock_filter.return_value = MockFilter([mock_cached1])

        prev = {
            'var_name': 'tas',
            'axis_map': {
                self.uris[0]: {
                    'time': slice(0, 122),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
            },
        }

        expected = prev.copy()

        expected['cached'] = {
            self.uris[0]: None,
        }

        data = preprocess.check_cache(prev, self.uris[0])

        self.assertEqual(data, expected)

    @mock.patch('wps.models.Cache.objects.filter')
    def test_check_cache_valid(self, mock_filter):
        mock_cached1 = mock.MagicMock()
        type(mock_cached1).url = mock.PropertyMock(return_value='file:///test_cached.nc')

        mock_filter.return_value = MockFilter([mock_cached1])

        prev = {
            'var_name': 'tas',
            'axis_map': {
                self.uris[0]: {
                    'time': slice(0, 122),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
            },
        }

        expected = prev.copy()

        expected['cached'] = {
            self.uris[0]: 'file:///test_cached.nc',
        }

        data = preprocess.check_cache(prev, self.uris[0])

        self.assertEqual(data, expected)

    def test_check_cache(self):
        data = preprocess.check_cache({}, self.uris[0])

        expected = {
            'cached': {
                self.uris[0]: None
            }
        }

        self.assertEqual(data, expected)

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
    @mock.patch('cdms2.open')
    def test_map_axis_values(self, mock_load, mock_open):
        self.maxDiff = None
        preprocess.get_uri = mock.MagicMock(return_value=self.uris[0])

        mock_time = mock.MagicMock()
        type(mock_time).id = mock.PropertyMock(return_value='time')
        type(mock_time).shape = mock.PropertyMock(side_effect=[(122,),(120,),(120,)])
        type(mock_time.clone.return_value).id = mock.PropertyMock(return_value='time')
        mock_time.clone.return_value.mapInterval.side_effect = [(0, 122),(0, 120),(0, 120)]

        mock_lat = mock.MagicMock()
        type(mock_lat).id = mock.PropertyMock(return_value='lat')
        type(mock_lat).shape = mock.PropertyMock(return_value=(100,))
        mock_lat.mapInterval.return_value = (0, 100)

        mock_lon = mock.MagicMock()
        type(mock_lon).id = mock.PropertyMock(return_value='lon')
        type(mock_lon).shape = mock.PropertyMock(return_value=(200,))
        mock_lon.mapInterval.return_value = (0, 200)

        preprocess.get_axis = mock.MagicMock(return_value=mock_time)

        preprocess.get_remaining_axes = mock.MagicMock(return_value=[mock_lat, mock_lon])

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
            }
        }

        data = preprocess.map_axis_values(self.units[0], self.uris[0], 'tas', 'time', self.domain1, self.user.id)

        self.assertEqual(data, expected)

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('wps.tasks.preprocess.get_axis')
    @mock.patch('cdms2.open')
    def test_determine_base_units_bad_user(self, mock_load, mock_get, mock_open):
        with self.assertRaises(wps.WPSError):
            check = preprocess.determine_base_units([], 'tas', 'time', 1337)

    @mock.patch('wps.tasks.credentials.load_certificate')
    @mock.patch('wps.tasks.preprocess.get_axis')
    @mock.patch('cdms2.open')
    def test_determine_base_units_none(self, mock_load, mock_get, mock_open):
        with self.assertRaises(wps.WPSError):
            check = preprocess.determine_base_units([], 'tas', 'time', self.user.id)

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
