#! /usr/bin/env python

import mock
from django import test

from wps import backends
from wps import models
from wps import settings

class LocalBackendTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json']

    def setUp(self):
        models.Process.objects.all().delete()

        self.backend = backends.Backend.get_backend('Local')

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

class BackendsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json']

    def test_workflow(self):
        backend = backends.Backend()

        with self.assertRaises(NotImplementedError):
            backend.workflow(None, {}, {}, {})

    def test_execute(self):
        backend = backends.Backend()

        with self.assertRaises(NotImplementedError):
            backend.execute('', {}, {}, {})

    def test_populate_processes(self):
        backend = backends.Backend()

        with self.assertRaises(NotImplementedError):
            backend.populate_processes()

    def test_initialize(self):
        backend = backends.Backend()

        backend.initialize()

    def test_add_process_duplicate(self):
        backend = backends.Backend()

        with self.assertNumQueries(5):
            backend.add_process('id', 'name')

        with self.assertNumQueries(5):
            backend.add_process('id', 'name')

    def test_add_process_without_abstract(self):
        backend = backends.Backend()

        with self.assertNumQueries(5):
            backend.add_process('id', 'name')

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
        expected = ['Local', 'Ophidia', 'EDAS']

        self.assertEqual(backends.Backend.registry.keys(), expected)
