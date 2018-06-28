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

        self.execute = {
            'base_units': 'days since 1990-1-1 0',
            'var_name': 'tas',
            'units': {
                self.uris[0]: 'days since 1990-1-1 0',
                self.uris[1]: 'days since 2000-1-1 0',
            },
            'mapped': {
                self.uris[0]: {
                    'time': slice(0, 122),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                }
            },
            'cached': {
                self.uris[0]: 'file:///test1_cached.nc',
            },
            'chunks': {
                self.uris[0]: {
                    'time': [
                        slice(0, 10),
                        slice(10, 20),
                    ],
                },
            },
            'preprocess': True,
            'workflow': False,
            'root': 'subset',
            'operation': self.operation,
            'domain': self.domain,
            'variable': self.variable,
            'user_id': 100,
            'job_id': 200,
        }

        self.execute_multiple = {
            'base_units': 'days since 1990-1-1 0',
            'var_name': 'tas',
            'units': {
                self.uris[0]: 'days since 1990-1-1 0',
                self.uris[1]: 'days since 2000-1-1 0',
            },
            'mapped': {
                self.uris[0]: {
                    'time': slice(0, 122),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                },
                self.uris[1]: {
                    'time': slice(0, 87),
                    'lat': slice(0, 100),
                    'lon': slice(0, 200),
                }
            },
            'cached': {
                self.uris[0]: 'file:///test1_cached.nc',
                self.uris[1]: 'file:///test1_cached.nc',
            },
            'chunks': {
                self.uris[0]: {
                    'time': [
                        slice(0, 10),
                        slice(10, 20),
                    ],
                },
                self.uris[1]: {
                    'time': [
                        slice(30, 40),
                        slice(40, 42),
                    ],
                },
            },
            'preprocess': True,
            'workflow': False,
            'root': 'subset',
            'operation': self.operation,
            'domain': self.domain,
            'variable': self.variable,
            'user_id': 100,
            'job_id': 200,
        }

    def test_populate_processes(self):
        count = len(tasks.REGISTRY)

        self.backend.populate_processes()

        self.assertEqual(len(self.backend.processes), count)

    def test_initialize(self):
        with self.assertNumQueries(0):
            self.backend.initialize()
