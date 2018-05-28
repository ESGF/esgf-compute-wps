import os
import shutil

import cwt
import mock
from django import test

from . import helpers
from wps import models
from wps import WPSError
from wps.tasks import cdat
from wps.tasks import edas

class EDASTaskTestCase(test.TestCase):
    fixtures = ['processes.json', 'servers.json', 'users.json']

    def setUp(self):
        self.user = models.User.objects.all()[0]

        self.server = models.Server.objects.all()[0]

        self.process = models.Process.objects.all()[0]

    def test_check_exceptions_error(self):
        with self.assertRaises(WPSError) as e:
            edas.check_exceptions('!<response><exceptions><exception>error</exception></exceptions></response>')

    def test_check_exceptions(self):
        edas.check_exceptions('!<response></response>')

    def test_listen_edas_output_timeout(self):
        mock_self = mock.MagicMock()

        mock_poller = mock.MagicMock(**{'poll.return_value': {}})

        mock_job = mock.MagicMock()

        with self.assertRaises(WPSError) as e:
            edas.listen_edas_output(mock_self, mock_poller, mock_job)

    def test_listen_edas_output_heartbeat(self):
        mock_self = mock.MagicMock()

        mock_poller = mock.MagicMock(**{'poll.return_value': [(mock.MagicMock(**{'recv.side_effect': ['response', 'file']}), 0)]})

        mock_job = mock.MagicMock()

        result = edas.listen_edas_output(mock_self, mock_poller, mock_job)

        self.assertIsInstance(result, str)
        self.assertEqual(result, 'file')

    def test_listen_edas_output(self):
        mock_self = mock.MagicMock()

        mock_poller = mock.MagicMock(**{'poll.return_value': [(mock.MagicMock(**{'recv.return_value': 'file'}), 0)]})

        mock_job = mock.MagicMock()

        result = edas.listen_edas_output(mock_self, mock_poller, mock_job)

        self.assertIsInstance(result, str)
        self.assertEqual(result, 'file')

    @mock.patch('wps.tasks.edas.cdms2.open')
    @mock.patch('shutil.move')
    @mock.patch('wps.tasks.edas.listen_edas_output')
    @mock.patch('wps.tasks.edas.initialize_socket')
    def test_edas_submit_listen_failed(self, mock_init_socket, mock_listen, mock_move, mock_open):
        mock_open.return_value = mock.MagicMock()

        mock_open.return_value.__enter__.return_value.variables.keys.return_value = ['tas']

        mock_job = mock.MagicMock()

        mock_listen.return_value = None

        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'},
        }

        operation = {
            'name': 'EDAS.sum',
            'input': ['v0'],
            'axes': 'time'
        }

        with self.assertRaises(WPSError) as e:
            edas.edas_submit({}, variables, {}, operation, user_id=self.user.id, job_id=0)

    @mock.patch('wps.tasks.edas.process.Process')
    @mock.patch('wps.tasks.edas.cdms2.open')
    @mock.patch('shutil.move')
    @mock.patch('wps.tasks.edas.listen_edas_output')
    @mock.patch('wps.tasks.edas.initialize_socket')
    def test_edas_submit(self, mock_init_socket, mock_listen, mock_move, mock_open, mock_process):
        mock_open.return_value = mock.MagicMock()

        mock_open.return_value.__enter__.return_value.variables.keys.return_value = ['tas']

        mock_job = mock.MagicMock()

        variables = {
            'v0': {'id': 'tas|v0', 'uri': 'file:///test.nc'},
        }

        operation = {
            'name': 'EDAS.sum',
            'input': ['v0'],
            'axes': 'time'
        }

        result = edas.edas_submit({}, variables, {}, operation, user_id=self.user.id, job_id=0)

        self.assertIsInstance(result, dict)

        calls = mock_init_socket.call_args_list

        self.assertEqual(calls[0][0][3], 5670)
        self.assertEqual(calls[1][0][3], 5671)
