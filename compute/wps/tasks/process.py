#! /usr/bin/env python

from wps import models
from wps import WPSError
from wps.tasks import credentials

class Process(object):
    def __init__(self):
        pass

    def initialize(self, user_id, job_id):
        try:
            self.user = models.User.objects.get(pk=user_id)
        except models.User.DoesNotExist:
            raise WPSError('User with id "{id}" does not exist', id=user_id)

        try:
            self.job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise WPSError('Job with id "{id}" does not exist', id=job_id)

        credentials.load_certificate(self.user)

    def retrieve(self, fm, operation):
        pass

    def process(self, fm, operation):
        pass
