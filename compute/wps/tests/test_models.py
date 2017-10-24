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
