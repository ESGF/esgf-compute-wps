#! /usr/bin/env python

import random

from wps import models

from .common import CommonTestCase

class JobViewsTestCase(CommonTestCase):

    def setUp(self):
        self.job_user = models.User.objects.create_user('job', 'job@gmail.com', 'job')

        self.jobs = []

        for _ in xrange(20):
            job = models.Job.objects.create(server=self.server,
                                      user=self.job_user,
                                      process=random.choice(self.processes))

            job.accepted()

            job.started()

            for i in xrange(random.randint(2, 6)):
                job.update_status('Progress {}'.format(i))

            if random.random() > 0.5:
                job.succeeded('Success')
            else:
                job.failed('Error')

            self.jobs.append(job)

    def tearDown(self):
        self.job_user.delete()

        models.Job.objects.all().delete()

    def test_job_auth(self):
        self.client.login(username='job', password='job')

        response = self.client.get('/wps/jobs/{}/'.format(self.jobs[0].id))

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(len(data['data']), 3)

    def test_job(self):
        response = self.client.get('/wps/jobs/0/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')

    def test_jobs_index(self):
        self.client.login(username='job', password='job')
    
        response = self.client.get('/wps/jobs/', {'index': 5})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data']['count'], 15)
        self.assertEqual(len(data['data']['jobs']), 15)

    def test_jobs_limit(self):
        self.client.login(username='job', password='job')
    
        response = self.client.get('/wps/jobs/', {'limit': 10})

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data']['count'], 10)
        self.assertEqual(len(data['data']['jobs']), 10)

    def test_jobs_auth(self):
        self.client.login(username='job', password='job')
    
        response = self.client.get('/wps/jobs/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data']['count'], 20)
        self.assertEqual(len(data['data']['jobs']), 20)

    def test_jobs(self):
        response = self.client.get('/wps/jobs/')

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertEqual(data['status'], 'failed')
        self.assertEqual(data['error'], 'Unauthorized access')
