#! /usr/bin/env python

import random

from django import test

from wps import models

class JobViewsTestCase(test.TestCase):
    fixtures = ['users.json', 'processes.json', 'servers.json', 'jobs.json']

    def setUp(self):
        self.user = models.User.objects.all()[0]

        self.jobs = models.Job.objects.all()

    def test_job_auth(self):
        self.client.login(username=self.user.username, password=self.user.username)

        response = self.client.get('/wps/jobs/{}/'.format(self.jobs[0].id))

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(len(data['data']), 2)

    def test_job(self):
        response = self.client.get('/wps/jobs/0/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_jobs_index(self):
        self.client.login(username=self.user.username, password=self.user.username)
    
        response = self.client.get('/wps/jobs/', {'index': 5})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(len(data['data']), 42)

    def test_jobs_limit(self):
        self.client.login(username=self.user.username, password=self.user.username)
    
        response = self.client.get('/wps/jobs/', {'limit': 10})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(len(data['data']), 42)

    def test_jobs_auth(self):
        self.client.login(username=self.user.username, password=self.user.username)
    
        response = self.client.get('/wps/jobs/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(len(data['data']), 42)

    def test_jobs(self):
        response = self.client.get('/wps/jobs/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
