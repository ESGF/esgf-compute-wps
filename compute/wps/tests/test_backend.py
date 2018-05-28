#! /usr/bin/env python

import mock
from django import test

from wps import backends
from wps import models

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

    def test_add_process_without_abstract(self):
        backend = backends.Backend()

        backend.add_process('id', 'name')

        self.assertEqual(len(backend.processes), 1)

    def test_add_process(self):
        backend = backends.Backend()

        backend.add_process('id', 'name', 'abstract')

        self.assertEqual(len(backend.processes), 1)

    def test_get_backend_does_not_exist(self):
        backend = backends.Backend.get_backend('DE')

        self.assertIsNone(backend)

    def test_get_backend(self):
        backend = backends.Backend.get_backend('CDAT')

        self.assertEqual(backend, backends.Backend.registry['CDAT'])

    def test_registry(self):
        expected = ['CDAT', 'Ophidia', 'EDAS']

        self.assertEqual(backends.Backend.registry.keys(), expected)
