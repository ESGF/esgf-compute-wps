import hashlib
import json

import cdms2
import mock
from django import test
from django.conf import settings

from wps import helpers
from wps import models

class CacheModelTestCase(test.TestCase):

    def setUp(self):
        dimensions = {
            'var_name': 'tas',
            'time': slice(25, 125),
            'lat': slice(50, 75),
            'lon': slice(100, 125),
        }

        dimensions_sans_var_name = {
            'time': slice(25, 125),
            'lat': slice(50, 75),
            'lon': slice(100, 125),
        }

        self.cached = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions,
                                  default=helpers.json_dumps_default),
            size=120000,
        )

        self.invalid_dimensions = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions='dasdasd',
            size=120000,
        )

        self.missing_var_name = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions_sans_var_name,
                                  default=helpers.json_dumps_default),
            size=120000,
        )

        self.mock_time = mock.MagicMock()
        type(self.mock_time).id = mock.PropertyMock(return_value='time')
        type(self.mock_time).shape = mock.PropertyMock(return_value=(100,))

        self.mock_lat = mock.MagicMock()
        type(self.mock_lat).id = mock.PropertyMock(return_value='lat')
        type(self.mock_lat).shape = mock.PropertyMock(return_value=(25,))

        self.mock_lon = mock.MagicMock()
        type(self.mock_lon).id = mock.PropertyMock(return_value='lon')
        type(self.mock_lon).shape = mock.PropertyMock(return_value=(25,))
        
        self.mock_lev = mock.MagicMock()
        type(self.mock_lev).id = mock.PropertyMock(return_value='lev')
        type(self.mock_lev).shape = mock.PropertyMock(return_value=(25,))


    @mock.patch('os.path.exists')
    @mock.patch('cdms2.open')
    def test_valid_local_missing(self, mock_open, mock_exist):
        mock_open.return_value.variables.__contains__.return_value = True
        mock_open.return_value.__getitem__.return_value.getAxisIndex.return_value = 1
        mock_open.return_value.__getitem__.return_value.getAxis.side_effect = [
            self.mock_time,
            self.mock_lat,
            self.mock_lon,
        ]

        mock_exist.return_value = False

        result = self.cached.valid

        self.assertFalse(result)

    @mock.patch('os.path.exists')
    @mock.patch('cdms2.open')
    def test_valid_invalid_dimensions(self, mock_open, mock_exist):
        mock_open.return_value.variables.__contains__.return_value = True
        mock_open.return_value.__getitem__.return_value.getAxisIndex.return_value = 1
        mock_open.return_value.__getitem__.return_value.getAxis.side_effect = [
            self.mock_time,
            self.mock_lat,
            self.mock_lon,
        ]

        mock_exist.return_value = True

        result = self.invalid_dimensions.valid

        self.assertFalse(result)

    @mock.patch('os.path.exists')
    @mock.patch('cdms2.open')
    def test_valid_missing_cdms2_error(self, mock_open, mock_exist):
        mock_open.side_effect = cdms2.CDMSError()

        mock_exist.return_value = True

        result = self.cached.valid

        self.assertFalse(result)

    @mock.patch('os.path.exists')
    @mock.patch('cdms2.open')
    def test_valid_missing_var_name(self, mock_open, mock_exist):
        mock_open.return_value.variables.__contains__.return_value = True
        mock_open.return_value.__getitem__.return_value.getAxisIndex.return_value = 1
        mock_open.return_value.__getitem__.return_value.getAxis.side_effect = [
            self.mock_time,
            self.mock_lat,
            self.mock_lon,
        ]

        mock_exist.return_value = True

        result = self.missing_var_name.valid

        self.assertFalse(result)

    @mock.patch('os.path.exists')
    @mock.patch('cdms2.open')
    def test_valid_missing_variable(self, mock_open, mock_exist):
        mock_open.return_value.variables.__contains__.return_value = False
        mock_open.return_value.__getitem__.return_value.getAxisIndex.return_value = 1
        mock_open.return_value.__getitem__.return_value.getAxis.side_effect = [
            self.mock_time,
            self.mock_lat,
            self.mock_lon,
        ]

        mock_exist.return_value = True

        result = self.cached.valid

        self.assertFalse(result)

    @mock.patch('os.path.exists')
    @mock.patch('cdms2.open')
    def test_valid_missing_axis(self, mock_open, mock_exist):
        mock_open.return_value.variables.__contains__.return_value = True
        mock_open.return_value.__getitem__.return_value.getAxisIndex.return_value = -1
        mock_open.return_value.__getitem__.return_value.getAxis.side_effect = [
            self.mock_time,
            self.mock_lat,
            self.mock_lon,
        ]

        mock_exist.return_value = True

        result = self.cached.valid

        self.assertFalse(result)

    @mock.patch('os.path.exists')
    @mock.patch('cdms2.open')
    def test_valid_mismatch_size(self, mock_open, mock_exist):
        type(self.mock_time).shape = mock.PropertyMock(return_value=(500,))

        mock_open.return_value.variables.__contains__.return_value = True
        mock_open.return_value.__getitem__.return_value.getAxisIndex.return_value = 1
        mock_open.return_value.__getitem__.return_value.getAxis.side_effect = [
            self.mock_time,
            self.mock_lat,
            self.mock_lon,
        ]

        mock_exist.return_value = True

        result = self.cached.valid

        self.assertFalse(result)

    @mock.patch('os.path.exists')
    @mock.patch('cdms2.open')
    def test_valid(self, mock_open, mock_exist):
        mock_open.return_value.variables.__contains__.return_value = True
        mock_open.return_value.__getitem__.return_value.getAxisIndex.return_value = 1
        mock_open.return_value.__getitem__.return_value.getAxis.side_effect = [
            self.mock_lat,
            self.mock_lon,
            self.mock_time,
        ]

        mock_exist.return_value = True

        result = self.cached.valid

        self.assertTrue(result)

    def test_cache_is_superset_lower_out_bounds(self):
        dimensions = {
            'time': slice(50, 125),
            'lat': slice(0, 65),
            'lon': slice(110, 125),
        }

        result = self.cached.is_superset(dimensions)

        self.assertFalse(result)

    def test_cache_is_superset_upper_out_bounds(self):
        dimensions = {
            'time': slice(50, 500),
            'lat': slice(53, 65),
            'lon': slice(110, 125),
        }

        result = self.cached.is_superset(dimensions)

        self.assertFalse(result)

    def test_cache_is_superset(self):
        dimensions = {
            'time': slice(50, 125),
            'lat': slice(53, 65),
            'lon': slice(110, 125),
        }

        result = self.cached.is_superset(dimensions)

        self.assertTrue(result)

    def test_cache_local_path(self):
        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions='',
            size=1200000,
        )

        filename_hash = hashlib.sha256(cache.uid+cache.dimensions).hexdigest()

        expected = '{}/{}.nc'.format(settings.WPS_CACHE_PATH, filename_hash)

        local_path = cache.local_path

        self.assertEqual(local_path, expected) 
