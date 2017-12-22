import os
import shutil

import cwt
import mock
from django import test

from . import helpers
from wps import models
from wps import settings
from wps.tasks import cdat
from wps.tasks import edas

class EDASTaskTestCase(test.TestCase):
    fixtures = ['processes.json', 'servers.json', 'users.json']

    def setUp(self):
        settings.EDAS_TIMEOUT = 5

        settings.EDAS_HOST = 'unknown'

        self.user = models.User.objects.all()[0]

        self.server = models.Server.objects.all()[0]

        self.process = models.Process.objects.all()[0]

    def test_listen_edas_output(self):
        job = models.Job.objects.create(server=self.server, user=self.user, process=self.process)

        job.accepted()

        job.started()

        expected_path = '/data/temp.nc'

        mock_socket = mock.Mock()

        mock_socket.recv.return_value = 'file!{}'.format(expected_path)

        poller = mock.Mock()

        poller.poll.return_value = [tuple([mock_socket, 1])]

        edas_output_path = edas.listen_edas_output(poller, job)

        self.assertEqual(edas_output_path, expected_path)

    def test_check_exceptions(self):
        data = "!<response><exceptions><exception>error</exception></exceptions></response>"

        with self.assertRaises(Exception):
            edas.check_exceptions(data)

    def test_edas_submit(self):
        job = models.Job.objects.create(server=self.server, user=self.user, process=self.process)

        params = {
            'user_id': self.user.id,
            'job_id': job,
        }

        operation = cwt.Process(identifier='CDSpark.max')

        input_path = os.path.join(os.getcwd(), 'test1.nc')

        variables = [
            cwt.Variable(input_path, 'tas')
        ]

        data_inputs = cwt.WPS('').prepare_data_inputs(operation, variables, None)

        with self.assertNumQueries(1), self.assertRaises(Exception):
            edas.edas_submit(data_inputs, 'CDSpark.max', **params)

