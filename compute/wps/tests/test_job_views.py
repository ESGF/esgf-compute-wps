#! /usr/bin/env python

import random

from django import test

from . import helpers
from wps import models

class JobViewsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json', 'jobs.json']

    def setUp(self):
        self.user = models.User.objects.all()[0]

        self.jobs = models.Job.objects.all()

    def test_job_multiple_update(self):
        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/api/jobs/{}/'.format(self.jobs[0].id), {'update': 'true'})

        data = helpers.check_success(self, response)

        self.assertEqual(len(data['data']), 2)

        response = self.client.get('/api/jobs/{}/'.format(self.jobs[0].id), {'update': 'true'})

        data = helpers.check_success(self, response)

        self.assertEqual(len(data['data']), 0)

    def test_job_update(self):
        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/api/jobs/{}/'.format(self.jobs[0].id), {'update': 'true'})

        data = helpers.check_success(self, response)

        self.assertEqual(len(data['data']), 2)

    def test_job_no_status(self):
        server = models.Server.objects.get(host='default')

        process = models.Process.objects.create(identifier='CDAT.test', backend='Local', version='devel')

        job = models.Job.objects.create(server=server, user=self.user, process=process)

        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/api/jobs/{}/'.format(job.id))

        data = helpers.check_success(self, response)

        self.assertEqual(len(data['data']), 0)

    def test_job(self):
        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/api/jobs/{}/'.format(self.jobs[0].id))

        data = helpers.check_success(self, response)

        self.assertEqual(len(data['data']), 2)

    def test_job_missing_authentication(self):
        response = self.client.get('/api/jobs/0/')

        helpers.check_failed(self, response)

    def test_jobs(self):
        self.client.login(username=self.user.username, password=self.user.username)
    
        response = self.client.get('/api/jobs/')

        data = helpers.check_success(self, response)
        
        self.assertEqual(len(data['data']), 5)

    def test_jobs_missing_authentication(self):
        response = self.client.get('/api/jobs/')

        helpers.check_failed(self, response)
