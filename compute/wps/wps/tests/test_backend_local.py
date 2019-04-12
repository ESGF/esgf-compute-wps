#! /usr/bin/env python

import cwt
import mock
from django import test

from wps import backends
from wps import models
from wps.tasks.base import REGISTRY

class LocalBackendTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json']

    def setUp(self):
        self.backend = backends.CDAT()

        models.Process.objects.filter(backend='CDAT').delete()

        self.server = models.Server.objects.get(host='default')

        self.user = models.User.objects.all()[0]

    def test_populate_processes(self):
        count = len(REGISTRY)

        self.backend.populate_processes()

        self.assertEqual(len(self.backend.processes), count)
