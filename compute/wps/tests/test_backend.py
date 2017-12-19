#! /usr/bin/env python

import mock
from django import test

from wps import backends
from wps import models
from wps import settings

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
