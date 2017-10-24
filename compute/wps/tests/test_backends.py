#! /usr/bin/env python

import mock
from django import test

from wps import backends
from wps import models
from wps import settings

class EDASBackendTestCase(test.TestCase):

    def setUp(self):
        self.server = models.Server.objects.create(host='default')

        self.user = models.User.objects.create_user('local', 'local@gmail.com', 'local')

        self.backend = backends.Backend.get_backend('EDAS')

    def tearDown(self):
        self.server.delete()

        self.user.delete()

    def test_execute(self):
        settings.EDAS_HOST = 'Unknown'

        process = models.Process.objects.create(identifier='CDSpark.max', backend='EDAS')

        job = models.Job.objects.create(server=self.server, user=self.user, process=process)

        self.backend.execute('CDSpark.max', {}, {}, {}, user=self.user, job=job)

    def test_populate_processes(self):
        settings.EDAS_HOST = 'Unknown'

        with self.assertNumQueries(1):
            self.backend.populate_processes()

    def test_initialize(self):
        with self.assertNumQueries(0):
            self.backend.initialize()

class LocalBackendTestCase(test.TestCase):

    def setUp(self):
        self.server = models.Server.objects.create(host='default')

        self.user = models.User.objects.create_user('local', 'local@gmail.com', 'local')

        self.backend = backends.Backend.get_backend('Local')

    def tearDown(self):
        self.server.delete()

        self.user.delete()

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
        with self.assertNumQueries(30):
            self.backend.populate_processes()

    def test_initialize(self):
        with self.assertNumQueries(0):
            self.backend.initialize()

class BackendsTestCase(test.TestCase):

    def setUp(self):
        models.Server.objects.create(host='default')

    def test_add_process(self):
        backend = backends.Backend()

        with self.assertNumQueries(5):
            backend.add_process('id', 'name', 'abstract')

    def test_get_backend_does_not_exist(self):
        backend = backends.Backend.get_backend('DE')

        self.assertIsNone(backend)

    def test_get_backend(self):
        backend = backends.Backend.get_backend('Local')

        self.assertEqual(backend, backends.Backend.registry['Local'])

    def test_registry(self):
        expected = ['Local', 'EDAS']

        self.assertEqual(backends.Backend.registry.keys(), expected)
