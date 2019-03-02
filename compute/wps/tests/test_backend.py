#! /usr/bin/env python

import cwt
import mock
from django import test

from wps import backends
from wps import models

class BackendsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json']

    def test_add_process_without_abstract(self):
        backend = backends.Backend()

        backend.add_process('id', {})

        self.assertEqual(len(backend.processes), 1)

    def test_add_process_not_data_inputs(self):
        backend = backends.Backend()

        backend.add_process('id', {}, abstract='abstract')

        self.assertEqual(len(backend.processes), 1)

    def test_add_process(self):
        backend = backends.Backend()

        backend.add_process('id', {}, abstract='abstract')

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
