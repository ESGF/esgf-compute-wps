from django import test

from wps import models

class ModelsTest(test.TestCase):
    def setUp(self):
        models.Cache.objects.create(uid='test0',
                                    url='http://test/test1',
                                    dimensions='',
                                    size=1000000)

        models.Cache.objects.create(uid='test1',
                                    url='http://test/test2',
                                    dimensions='',
                                    size=50000)

    def test_cache_free(self):
        models.Cache.free(65000)


    def test_cache_size(self):
        size = models.Cache.total_size()

        self.assertEqual(size, 1050000)
