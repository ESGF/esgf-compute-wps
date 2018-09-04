#! /usr/bin/env python

import unittest

import cwt
import mock
from django import test
from django.conf import settings

from wps import backends
from wps import models

#class EDASBackendTestCase(test.TestCase):
#    fixtures = ['users.json', 'processes.json', 'servers.json']
#
#    def setUp(self):
#        self.backend = backends.EDAS()
#
#        models.Process.objects.filter(backend=self.backend.NAME).delete()
#
#        self.server = models.Server.objects.get(host='default')
#
#        self.user = models.User.objects.all()[0]
#
#    @mock.patch('wps.backends.edas.zmq.Context.instance')
#    def test_get_capabilities_communication_error(self, mock_context):
#        mock_socket = mock.Mock(**{'poll.return_value': 0})
#
#        mock_context.return_value = mock.Mock(**{'socket.return_value': mock_socket})
#
#        with self.assertRaises(backends.EDASCommunicationError) as e:
#            self.backend.get_capabilities()
#
#        self.assertEqual(str(e.exception), 
#                         str(backends.EDASCommunicationError(settings.WPS_EDAS_HOST, 
#                                                             settings.WPS_EDAS_REQ_PORT)))
#
#    @mock.patch('wps.backends.edas.zmq.Context.instance')
#    def test_get_capabilities(self, mock_context):
#        mock_data = 'header!response'
#
#        mock_socket = mock.Mock(**{'poll.return_value': 1, 'recv.return_value': mock_data})
#
#        mock_context.return_value = mock.Mock(**{'socket.return_value': mock_socket})
#
#        response = self.backend.get_capabilities()
#
#        self.assertIsNotNone(response)
#        self.assertEqual(response, 'response')
#
#    def test_execute(self):
#        process = models.Process.objects.create(identifier='CDSpark.max', backend='EDAS')
#
#        job = models.Job.objects.create(server=self.server, user=self.user, process=process)
#
#        domain = cwt.Domain([
#            cwt.Dimension('time', 0, 200)
#        ], name='d0')
#
#        domains = {'d0': domain}
#
#        var = cwt.Variable('file:///test.nc', 'tas', name='v0')
#
#        variables = {'v0': var}
#
#        proc = cwt.Process(identifier='CDSpark.max', name='max')
#
#        proc.domain = domain
#
#        proc.add_inputs('v0')
#
#        operations = {'max': proc}
#
#        task = self.backend.execute('CDSpark.max', variables, domains, operations, user=self.user, job=job)
#
#        self.assertIsNotNone(task)
#
#    def test_populate_processes(self):
#        self.backend.get_capabilities = mock.Mock(return_value='<response><processes><process><description id="test" title="Test">abstract</description></process></processes></response>')
#
#        settings.WPS_WPS_EDAS_ENABLED = True
#
#        with self.assertNumQueries(1):
#            self.backend.populate_processes()
#
#    def test_initialize(self):
#        with self.assertNumQueries(0):
#            self.backend.initialize()
