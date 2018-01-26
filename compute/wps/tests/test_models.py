import hashlib
import json

import mock
from django import test

from . import helpers
from wps import models
from wps import settings

class CacheModelTestCase(test.TestCase):

    @classmethod
    def setUpClass(cls):
        super(CacheModelTestCase, cls).setUpClass()

        cls.time = helpers.generate_time('days since 1990', 365)

    def test_cache_is_superset_invalid_time(self):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(100, 200),
            'spatial': {
                'lat': slice(0, 90),
                'lon': slice(0, 180),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        value = {
            'temporal': slice(100, 720),
            'spatial': {
                'lat': slice(20, 40),
                'lon': slice(20, 80),
            }
        }

        self.assertFalse(cache.is_superset(value))

    def test_cache_is_superset_invalid_spatial(self):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(100, 200),
            'spatial': {
                'lat': slice(0, 90),
                'lon': slice(0, 180),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        value = {
            'temporal': slice(120, 180),
            'spatial': {
                'lat': slice(20, 40),
                'lon': slice(20, 720),
            }
        }

        self.assertFalse(cache.is_superset(value))

    def test_cache_is_superset_missing_in_cache(self):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(100, 200),
            'spatial': {
                'lat': slice(0, 90),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        value = {
            'temporal': slice(120, 180),
            'spatial': {
                'lat': slice(20, 40),
                'lon': slice(20, 80),
            }
        }

        self.assertTrue(cache.is_superset(value))

    def test_cache_is_superset_missing_in_file(self):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(100, 200),
            'spatial': {
                'lat': slice(0, 90),
                'lon': slice(0, 180),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        value = {
            'temporal': slice(120, 180),
            'spatial': {
                'lon': slice(20, 80),
            }
        }

        self.assertFalse(cache.is_superset(value))

    def test_cache_is_superset(self):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(100, 200),
            'spatial': {
                'lat': slice(0, 90),
                'lon': slice(0, 180),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        value = {
            'temporal': slice(120, 180),
            'spatial': {
                'lat': slice(20, 40),
                'lon': slice(20, 80),
            }
        }

        self.assertTrue(cache.is_superset(value))

    @mock.patch('wps.models.os.path.exists')
    def test_cache_valid_does_not_exist(self, mock_exists):
        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=''
        )

        mock_exists.return_value = False

        self.assertFalse(cache.valid)

    @mock.patch('wps.models.os.path.exists')
    def test_cache_valid_missing_key(self, mock_exists):
        dimensions = {}

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        mock_exists.return_value = True

        self.assertFalse(cache.valid)

    @mock.patch('wps.models.os.path.exists')
    @mock.patch('wps.models.cdms2.open')
    def test_cache_valid_missing_variable(self, mock_open, mock_exists):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(100, 200),
            'spatial': {
                'lat': slice(0, 90),
                'lon': slice(0, 180),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        mock_exists.return_value = True

        mock_open.return_value.__enter__.return_value.__contains__.return_value = False

        self.assertFalse(cache.valid)

    @mock.patch('wps.models.os.path.exists')
    @mock.patch('wps.models.cdms2.open')
    def test_cache_valid_missing_temporal_axis(self, mock_open, mock_exists):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(100, 200),
            'spatial': {
                'lat': slice(0, 90),
                'lon': slice(0, 180),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        mock_exists.return_value = True

        mock_context = mock_open.return_value.__enter__.return_value

        mock_context.__contains__.return_value = True

        mock_context.__getitem__.return_value.getTime.return_value = None

        self.assertFalse(cache.valid)

    @mock.patch('wps.models.os.path.exists')
    @mock.patch('wps.models.cdms2.open')
    def test_cache_valid_invalid_temporal_axis(self, mock_open, mock_exists):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(100, 200),
            'spatial': {
                'lat': slice(0, 90),
                'lon': slice(0, 180),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        mock_exists.return_value = True

        mock_context = mock_open.return_value.__enter__.return_value

        mock_context.__contains__.return_value = True

        mock_context.__getitem__.return_value.getTime.return_value = self.time

        self.assertFalse(cache.valid)

    @mock.patch('wps.models.os.path.exists')
    @mock.patch('wps.models.cdms2.open')
    def test_cache_valid_missing_spatial(self, mock_open, mock_exists):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(0, 365),
            'spatial': {
                'lat': slice(0, 90),
                'lon': slice(0, 180),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        mock_exists.return_value = True

        mock_context = mock_open.return_value.__enter__.return_value

        mock_context.__contains__.return_value = True

        mock_context.__getitem__.return_value.getTime.return_value = self.time

        mock_context.__getitem__.return_value.getAxisIndex.return_value = -1

        self.assertFalse(cache.valid)

    @mock.patch('wps.models.os.path.exists')
    @mock.patch('wps.models.cdms2.open')
    def test_cache_valid_invalid_spatial_size(self, mock_open, mock_exists):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(0, 365),
            'spatial': {
                'lat': slice(0, 90),
                'lon': slice(0, 180),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        mock_exists.return_value = True

        mock_context = mock_open.return_value.__enter__.return_value

        mock_context.__contains__.return_value = True

        mock_context.__getitem__.return_value.getTime.return_value = self.time

        mock_context.__getitem__.return_value.getAxisIndex.return_value = 1

        mock_context.__getitem__.return_value.getAxis.side_effect = [helpers.latitude, helpers.longitude]

        self.assertFalse(cache.valid)

    @mock.patch('wps.models.os.path.exists')
    @mock.patch('wps.models.cdms2.open')
    def test_cache_valid_tuple(self, mock_open, mock_exists):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(0, 365),
            'spatial': {
                'lat': slice(0, 180),
                'lon': slice(0, 360),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        mock_exists.return_value = True

        mock_context = mock_open.return_value.__enter__.return_value

        mock_context.variables = {'tas': True}

        mock_context.__getitem__.return_value.getTime.return_value = self.time

        mock_context.__getitem__.return_value.getAxisIndex.return_value = 1

        mock_context.__getitem__.return_value.getAxis.side_effect = [helpers.latitude, helpers.longitude]

        self.assertTrue(cache.valid)

    @mock.patch('wps.models.os.path.exists')
    @mock.patch('wps.models.cdms2.open')
    def test_cache_valid(self, mock_open, mock_exists):
        dimensions = {
            'variable': 'tas',
            'temporal': slice(0, 365),
            'spatial': {
                'lat': slice(0, 180),
                'lon': slice(0, 360),
            }
        }

        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=models.slice_default)
        )

        mock_exists.return_value = True

        mock_context = mock_open.return_value.__enter__.return_value

        mock_context.variables = {'tas': True}

        mock_context.__getitem__.return_value.getTime.return_value = self.time

        mock_context.__getitem__.return_value.getAxisIndex.return_value = 1

        mock_context.__getitem__.return_value.getAxis.side_effect = [helpers.latitude, helpers.longitude]

        self.assertTrue(cache.valid)

    def test_cache_local_path(self):
        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=''
        )

        filename_hash = hashlib.sha256(cache.uid+cache.dimensions).hexdigest()

        expected = '{}/{}.nc'.format(settings.CACHE_PATH, filename_hash)

        local_path = cache.local_path

        self.assertEqual(local_path, expected) 
