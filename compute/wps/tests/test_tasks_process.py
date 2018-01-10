#! /usr/bin/env python

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

    @mock.patch('wps.tasks.process.credentials.load_certificate')
    def test_initialize_missing_user(self, mock_load):
        job = models.Job.objects.create(server=self.server, user=self.user)

        proc = process.Process()

        with self.assertRaises(WPSError):
            proc.initialize(1000000, job.id)

    @mock.patch('wps.tasks.process.credentials.load_certificate')
    def test_initialize_missing_job(self, mock_load):
        proc = process.Process()

        with self.assertRaises(WPSError):
            proc.initialize(self.user.id, 1000000)

    @mock.patch('wps.tasks.process.credentials.load_certificate')
    def test_initialize(self, mock_load):
        job = models.Job.objects.create(server=self.server, user=self.user)

        proc = process.Process()

        proc.initialize(self.user.id, job.id)

        mock_load.assert_called_with(self.user)
