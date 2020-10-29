from __future__ import unicode_literals
from __future__ import division

from builtins import range
from builtins import object
import base64
import json
import logging
import random
import re
import string
import time
from urllib.parse import urlparse

from django.conf import settings
from django.contrib.auth.models import User
from django.db import IntegrityError
from django.db import models
from django.db.models import F
from django.db.models.query_utils import Q
from django.utils import timezone

from compute_wps import metrics
from compute_wps.util import wps_response

logger = logging.getLogger('compute_wps.models')

class Nonce(models.Model):
    state = models.CharField(max_length=128)
    redirect_uri = models.CharField(max_length=255)
    next = models.CharField(max_length=255)

class KeyCloakUserClient(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    access_token = models.CharField(max_length=512)

class File(models.Model):
    name = models.CharField(max_length=256)
    host = models.CharField(max_length=256)
    variable = models.CharField(max_length=64)
    url = models.TextField()
    requested = models.PositiveIntegerField(default=0)

    class Meta(object):
        unique_together = ('name', 'host', 'variable')

    def __str__(self):
        return f'Url {self.url} variable {self.variable}'

class UserFile(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    file = models.ForeignKey(File, on_delete=models.CASCADE)
    requested_date = models.DateTimeField(auto_now=True)
    requested = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f'User {self.user.pk} file {self.file.url}'

class Process(models.Model):
    identifier = models.CharField(max_length=128, blank=False)
    abstract = models.TextField()
    metadata = models.TextField()
    version = models.CharField(max_length=128, blank=False, default=None)

    class Meta(object):
        unique_together = (('identifier', 'version'),)

    def __str__(self):
        return f'Process {self.identifier}'

class UserProcess(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    process = models.ForeignKey(Process, on_delete=models.CASCADE)
    requested_date = models.DateTimeField(auto_now=True)
    requested = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f'User {self.user.pk} process {self.process.identifier}'

class Job(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    process = models.ForeignKey(Process, on_delete=models.CASCADE)
    datainputs = models.TextField()

    def __str__(self):
        return f'User {self.user.pk} process {self.process.identifier}'

    def accepted(self):
        status = self.status_set.create(status=wps_response.ProcessAccepted)

        return status.id

    def failed(self, exc):
        if not isinstance(exc, str):
            exc = str(exc)

        try:
            self.status_set.create(status=wps_response.ProcessFailed, exception=exc)
        except IntegrityError:
            logger.error('Failed status already exists')


class Status(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    updated_date = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=128)

    class Meta:
        unique_together = (('job', 'status'), )

    def __str__(self):
        return f'Job {self.job.pk} status {self.status}'

class Message(models.Model):
    status = models.ForeignKey(Status, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    percent = models.PositiveIntegerField(null=True)
    message = models.TextField(null=True)

    def __str__(self):
        return f'Job {self.status.job.pk} message {self.message} percent {self.percent}'
