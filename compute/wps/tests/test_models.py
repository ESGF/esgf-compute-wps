import hashlib
import json

import mock
from django import test
from django.conf import settings

from wps import helpers
from wps import models

class CacheModelTestCase(test.TestCase):

    def setUp(self):
        dimensions = {
            'time': slice(25, 125),
            'lat': slice(50, 75),
            'lon': slice(100, 125),
        }

        self.superset = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=json.dumps(dimensions, default=helpers.json_dumps_default)
        )

    def test_cache_is_superset_lower_out_bounds(self):
        dimensions = {
            'time': slice(50, 125),
            'lat': slice(0, 65),
            'lon': slice(110, 125),
        }

        result = self.superset.is_superset(dimensions)

        self.assertFalse(result)

    def test_cache_is_superset_upper_out_bounds(self):
        dimensions = {
            'time': slice(50, 500),
            'lat': slice(53, 65),
            'lon': slice(110, 125),
        }

        result = self.superset.is_superset(dimensions)

        self.assertFalse(result)

    def test_cache_is_superset(self):
        dimensions = {
            'time': slice(50, 125),
            'lat': slice(53, 65),
            'lon': slice(110, 125),
        }

        result = self.superset.is_superset(dimensions)

        self.assertTrue(result)

    def test_cache_local_path(self):
        cache = models.Cache.objects.create(
            uid='uid',
            url='file:///test1.nc',
            dimensions=''
        )

        filename_hash = hashlib.sha256(cache.uid+cache.dimensions).hexdigest()

        expected = '{}/{}.nc'.format(settings.WPS_CACHE_PATH, filename_hash)

        local_path = cache.local_path

        self.assertEqual(local_path, expected) 
