#! /usr/bin/env python

import cwt
import mock
from django import test

from wps import backends
from wps import models
from wps import settings
from wps import tasks

class LocalBackendTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json']

    def setUp(self):
        self.backend = backends.Local()

        models.Process.objects.filter(backend='Local').delete()

        self.server = models.Server.objects.get(host='default')

        self.user = models.User.objects.all()[0]

    def test_workflow(self):
        self.backend.populate_processes()

        process = models.Process.objects.get(identifier='CDAT.subset')

        job = models.Job.objects.create(server=self.server, user=self.user, process=process)

        var = cwt.Variable('file:///test.nc', 'tas', name='v0')

        variables = {'v0': var}

        domain = cwt.Domain([
            cwt.Dimension('time', 0, 200)
        ], name='d0')

        domains = {'d0': domain}

        proc1 = cwt.Process(identifier='CDAT.aggregate', name='aggregate')

        proc1.set_inputs('v0')

        proc2 = cwt.Process(identifier='CDAT.subset', name='subset')

        proc2.set_inputs('aggregate')

        proc3 = cwt.Process(identifier='CDSpark.max', name='max')

        proc3.set_inputs('subset')

        proc4 = cwt.Process(identifier='Oph.avg', name='avg')

        proc4.set_inputs('max')

        operations = {'aggregate': proc1, 'subset': proc2, 'max': proc3, 'avg': proc4}

        workflow = self.backend.workflow(proc4, variables, domains, operations, user=self.user, job=job)

    @mock.patch('wps.helpers.determine_queue')
    def test_execute(self, mock_determine):
        mock_determine.return_value = {
            'queue': 'priority.high',
            'exchange': 'priority',
            'routing_key': 'high',
        }

        self.backend.populate_processes()

        process = models.Process.objects.get(identifier='CDAT.subset')

        job = models.Job.objects.create(server=self.server, user=self.user, process=process)

        var = cwt.Variable('file:///test.nc', 'tas', name='v0')

        variables = {'v0': var}

        domain = cwt.Domain([
            cwt.Dimension('time', 0, 200)
        ], name='d0')

        domains = {'d0': domain}

        proc = cwt.Process(identifier='CDAT.subset', name='subset')

        operations = {'subset': proc}

        process = mock.MagicMock()
        process.id = 12

        kwargs = {
            'user': self.user,
            'job': job,
            'process': process,
            'estimate_size': 1000,
        }

        proc = self.backend.execute('CDAT.subset', variables, domains, operations, **kwargs)

        self.assertIsNotNone(proc)

    def test_populate_processes(self):
        count = len(tasks.REGISTRY)

        queries_per_process = 5

        with self.assertNumQueries(count * queries_per_process):
            self.backend.populate_processes()

    def test_initialize(self):
        with self.assertNumQueries(0):
            self.backend.initialize()
