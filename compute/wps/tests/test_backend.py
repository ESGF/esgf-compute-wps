#! /usr/bin/env python

import cwt
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

    def test_add_process_override_input_output(self):
        inputs = [cwt.wps.data_input_description('new_input', 'new_input', 'text/json', 1, 1)]

        outputs = [cwt.wps.process_output_description('new_output', 'new_output', 'text/json')]

        backend = backends.Backend()

        backend.add_process('id', 'name', abstract='abstract', data_inputs=inputs, process_outputs=outputs)

        self.assertEqual(len(backend.processes), 1)

        description = backend.processes[0]['description']

        self.assertIn('new_input', description)
        self.assertIn('new_output', description)

    def test_add_process_not_data_inputs(self):
        backend = backends.Backend()

        backend.add_process('id', 'name', abstract='abstract', data_inputs=[])

        self.assertEqual(len(backend.processes), 1)

        description = backend.processes[0]['description']

        self.assertIn('output', description)
        self.assertNotIn('variable', description)
        self.assertNotIn('domain', description)
        self.assertNotIn('operation', description)

    def test_add_process(self):
        backend = backends.Backend()

        backend.add_process('id', 'name', abstract='abstract')

        self.assertEqual(len(backend.processes), 1)

        description = backend.processes[0]['description']

        self.assertIn('output', description)
        self.assertIn('variable', description)
        self.assertIn('domain', description)
        self.assertIn('operation', description)

    def test_get_backend_does_not_exist(self):
        backend = backends.Backend.get_backend('DE')

        self.assertIsNone(backend)

    def test_get_backend(self):
        backend = backends.Backend.get_backend('CDAT')

        self.assertEqual(backend, backends.Backend.registry['CDAT'])

    def test_registry(self):
        expected = ['CDAT', 'Ophidia', 'EDAS']

        self.assertEqual(backends.Backend.registry.keys(), expected)
