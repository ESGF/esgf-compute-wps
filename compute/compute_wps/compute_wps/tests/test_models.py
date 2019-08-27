#! /usr/bin/env python

import mock
from django import test

from compute_wps import models

class ModelsFileTestCase(test.TestCase):
    fixtures = ['users.json', 'files.json']

    def setUp(self):
        self.file = models.File.objects.first()
        self.files = models.File.objects.all()

    def testFile(self):
        print("DEBUG...self.file.name: %r", self.file.name)
        for f in self.files:
            print("DEBUG DEBUG...name: %r", f.name)


class CacheModelTestCase(test.TestCase):
    pass
