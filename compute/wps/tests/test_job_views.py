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
