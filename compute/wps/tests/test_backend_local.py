#! /usr/bin/env python

import mock
from django import test

from wps import backends
from wps import models
from wps import settings

class LocalBackendTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json']

    def setUp(self):
        self.backend = backends.Local()

        models.Process.objects.filter(backend='Local').delete()

        self.server = models.Server.objects.get(host='default')

        self.user = models.User.objects.all()[0]

    @mock.patch('wps.backends.local.process.get_process')
    def test_execute(self, get_process_mock):
        si_mock = mock.Mock(**{'delay.return_value': True})

        process_mock = mock.Mock(**{'si.return_value': si_mock})

        get_process_mock.return_value = process_mock

        self.backend.populate_processes()

        process = models.Process.objects.get(identifier='CDAT.subset')

        job = models.Job.objects.create(server=self.server, user=self.user, process=process)

        with self.assertNumQueries(0):
            self.backend.execute('CDAT.subset', {}, {}, {}, user=self.user, job=job)

        get_process_mock.assert_called_once()
        get_process_mock.assert_called_with('CDAT.subset')

        self.assertEqual(len(process_mock.method_calls), 1)

        self.assertEqual(len(si_mock.method_calls), 1)

    def test_populate_processes(self):
        with self.assertNumQueries(10):
            self.backend.populate_processes()

    def test_initialize(self):
        with self.assertNumQueries(0):
            self.backend.initialize()

