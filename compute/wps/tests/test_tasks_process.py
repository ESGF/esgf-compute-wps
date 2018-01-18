#! /usr/bin/env python

import cwt
import mock
from django import test

from wps import models
from wps import WPSError
from wps.tasks import process

class ProcessTestCase(test.TestCase):

    fixtures = ['jobs.json', 'users.json', 'servers.json', 'processes.json']

    def setUp(self):
        self.user = models.User.objects.first()

        self.server = models.Server.objects.get(host='default')

        self.process = models.Process.objects.create(identifier='CDAT.test', server=self.server)

        self.job = models.Job.objects.create(server=self.server, user=self.user, process=self.process)

    @mock.patch('wps.tasks.process.cdms2.createGaussianGrid')
    def test_generate_grid_value_error(self, mock_gaussian):
        self.job.accepted()

        proc = process.Process('task_id')

        proc.user = self.user

        proc.job = self.job

        with self.assertRaises(WPSError):
            grid = proc.generate_grid(cwt.Gridder(grid='gaussian~32.0'))

    @mock.patch('wps.tasks.process.cdms2.createUniformGrid')
    def test_generate_grid_uniform_value_error(self, mock_uniform):
        self.job.accepted()

        proc = process.Process('task_id')

        proc.user = self.user

        proc.job = self.job

        with self.assertRaises(WPSError):
            grid = proc.generate_grid(cwt.Gridder(grid='uniform~32x64.0'))

    @mock.patch('wps.tasks.process.cdms2.createUniformGrid')
    def test_generate_grid_uniform(self, mock_uniform):
        self.job.accepted()

        proc = process.Process('task_id')

        proc.user = self.user

        proc.job = self.job

        grid = proc.generate_grid(cwt.Gridder(grid='uniform~32x64'))

        mock_uniform.assert_called_with(0, 32, 0, 0, 64, 0)

    @mock.patch('wps.tasks.process.cdms2.createGaussianGrid')
    def test_generate_grid_parse_error(self, mock_gaussian):
        self.job.accepted()

        proc = process.Process('task_id')

        proc.user = self.user

        proc.job = self.job

        with self.assertRaises(WPSError):
            grid = proc.generate_grid(cwt.Gridder(grid='something invalid'))

    @mock.patch('wps.tasks.process.cdms2.createGaussianGrid')
    def test_generate_grid(self, mock_gaussian):
        self.job.accepted()

        proc = process.Process('task_id')

        proc.user = self.user

        proc.job = self.job

        grid = proc.generate_grid(cwt.Gridder(grid='gaussian~32'))

        mock_gaussian.assert_called_with(32)

    def test_log(self):
        self.job.accepted()

        self.job.started()

        proc = process.Process('task_id')

        proc.user = self.user

        proc.job = self.job

        proc.job.update_status = mock.MagicMock()

        proc.log('some message {}', 'hello', percent=10)

        proc.job.update_status.assert_called_once()

    @mock.patch('wps.tasks.process.credentials.load_certificate')
    def test_process(self, mock_load):
        proc = process.Process('task_id')

        proc.initialize(self.user.id, self.job.id)

        self.assertEqual(proc.user, self.user)
        self.assertEqual(proc.job, self.job)
