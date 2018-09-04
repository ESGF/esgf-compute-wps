#! /usr/bin/env python

import cwt
import mock
from django import test

from wps import backends
from wps import models
from wps import tasks

class LocalBackendTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json']

    def setUp(self):
        self.backend = backends.CDAT()

        models.Process.objects.filter(backend='CDAT').delete()

        self.server = models.Server.objects.get(host='default')

        self.user = models.User.objects.all()[0]

    def test_populate_processes(self):
        count = len(tasks.REGISTRY)

        self.backend.populate_processes()

        self.assertEqual(len(self.backend.processes), count)

    def test_initialize(self):
        with self.assertNumQueries(0):
            self.backend.initialize()
