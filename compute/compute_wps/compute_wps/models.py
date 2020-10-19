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

from openid import association
from openid.store import interface
from openid.store import nonce

from compute_wps import metrics
from compute_wps.util import wps_response

logger = logging.getLogger('compute_wps.models')

ProcessAccepted = 'ProcessAccepted'
ProcessStarted = 'ProcessStarted'
ProcessPaused = 'ProcessPaused'
ProcessSucceeded = 'ProcessSucceeded'
ProcessFailed = 'ProcessFailed'

OAUTH2_TYPE = 'oauth2'
MPC_TYPE = 'myproxyclient'

class File(models.Model):
    name = models.CharField(max_length=256)
    host = models.CharField(max_length=256)
    variable = models.CharField(max_length=64)
    url = models.TextField()
    requested = models.PositiveIntegerField(default=0)

    class Meta(object):
        unique_together = ('name', 'host')

    @staticmethod
    def track(user, variable):
        url_parts = urlparse(variable.uri)

        parts = url_parts[2].split('/')

        file_obj, _ = File.objects.get_or_create(
            name=parts[-1],
            host=url_parts[1],
            defaults={
                'variable': variable.var_name,
                'url': variable.uri,
            }
        )

        file_obj.requested = F('requested') + 1

        file_obj.save()

        user_file_obj, _ = UserFile.objects.get_or_create(user=user, file=file_obj)

        user_file_obj.requested = F('requested') + 1

        user_file_obj.save()

    def to_json(self):
        return {
            'name': self.name,
            'host': self.host,
            'variable': self.variable,
            'url': self.url,
            'requested': self.requested
        }

    def __str__(self):
        return '{0.name}'.format(self)


class UserFile(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    file = models.ForeignKey(File, on_delete=models.CASCADE)
    requested_date = models.DateTimeField(auto_now=True)
    requested = models.PositiveIntegerField(default=0)

    def to_json(self):
        data = self.file.to_json()

        data['requested'] = self.requested

        data['requested_date'] = self.requested_date

        return data

    def __str__(self):
        return '{0.file.name}'.format(self)


class Auth(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)

    openid_url = models.CharField(max_length=256)
    type = models.CharField(max_length=64)
    cert = models.TextField()
    api_key = models.CharField(max_length=128)
    extra = models.TextField()

    def generate_api_key(self):
        self.api_key = ''.join(random.choice(string.ascii_letters+string.digits) for _ in range(64))

        self.save(update_fields=['api_key'])

    def update(self, type=None, certs=None, api_key=None, **kwargs):
        update_fields = []

        if type is not None:
            self.type = type

            update_fields.append('type')

        if certs is not None:
            self.cert = ''.join([x if isinstance(x, str) else x.decode() for x in certs])

            update_fields.append('cert')

        if api_key is not None:
            self.api_key = api_key

            update_fields.append('api_key')

        if len(kwargs) > 0:
            extra = json.loads(self.extra or '{}')

            extra.update(kwargs)

            self.extra = json.dumps(extra)

            update_fields.append('extra')

        self.save(update_fields=update_fields)

        self.refresh_from_db()

    def get(self, *keys):
        values = []

        data = json.loads(self.extra or '{}')

        for key in keys:
            values.append(data[key])

        return values

    def __str__(self):
        return '{0.openid_url} {0.type}'.format(self)


class Process(models.Model):
    identifier = models.CharField(max_length=128, blank=False)
    backend = models.CharField(max_length=128)
    abstract = models.TextField()
    metadata = models.TextField()
    version = models.CharField(max_length=128, blank=False, default=None)

    class Meta(object):
        unique_together = (('identifier', 'version'),)

    def decode_metadata(self):
        try:
            return json.loads(self.metadata)
        except ValueError:
            return {}

    def track(self, user):
        user_process_obj, _ = UserProcess.objects.get_or_create(user=user, process=self)

        user_process_obj.requested = F('requested') + 1

        user_process_obj.save()

    def to_json(self, usage=False):
        return {
            'identifier': self.identifier,
            'backend': self.backend,
        }

    def __str__(self):
        return '{0.identifier}'.format(self)


class UserProcess(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    process = models.ForeignKey(Process, on_delete=models.CASCADE)
    requested_date = models.DateTimeField(auto_now=True)
    requested = models.PositiveIntegerField(default=0)

    def to_json(self):
        data = self.process.to_json()

        data['requested'] = self.requested

        data['requested_date'] = self.requested_date

        return data

    def __str__(self):
        return '{0.process.identifier}'.format(self)


class Server(models.Model):
    host = models.CharField(max_length=128, unique=True)
    added_date = models.DateTimeField(auto_now_add=True)
    status = models.IntegerField(default=1)
    capabilities = models.TextField()

    processes = models.ManyToManyField(Process)

    def __str__(self):
        return '{0.host}'.format(self)


class Job(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    process = models.ForeignKey(Process, on_delete=models.CASCADE, null=True)
    expired = models.BooleanField(default=False)
    extra = models.TextField(null=True)

    @property
    def latest_status(self):
        latest = self.status.latest('created_date')

        return latest.status

    @property
    def accepted_on(self):
        accepted = self.status.filter(status=ProcessAccepted)

        if len(accepted) == 0:
            return 'Unknown'

        return accepted[0].created_date.isoformat()

    @property
    def elapsed(self):
        started = self.status.filter(status='ProcessStarted')

        ended = self.status.filter(Q(status='ProcessSucceeded') | Q(status='ProcessFailed'))

        if len(started) == 0 or len(ended) == 0:
            return 'No elapsed'

        elapsed = ended[0].created_date - started[0].created_date

        return '{}.{}'.format(elapsed.seconds, elapsed.microseconds)

    @property
    def report(self):
        latest = self.status.latest('updated_date')

        earliest = self.status.earliest('created_date')

        kwargs = {
            'status_location': settings.STATUS_URL.format(job_id=self.id),
            'instance': settings.EXTERNAL_WPS_URL,
            'latest': latest,
            'earliest': earliest,
            'process': self.process,
        }

        data_inputs = json.loads(self.extra)

        kwargs.update(data_inputs)

        return wps_response.execute(**kwargs)

    def accepted(self):
        status = self.status.create(status=ProcessAccepted)

        return status.id

    def failed(self, exc):
        if not isinstance(exc, str):
            exc = str(exc)

        try:
            self.status.create(status=ProcessFailed, exception=exc)
        except IntegrityError:
            logger.error('Failed status already exists')
        else:
            metrics.WPS_JOB_STATE.label(ProcessFailed).inc()


class Output(models.Model):
    job = models.ForeignKey(Job, related_name='output', on_delete=models.CASCADE)

    path = models.URLField()


class Status(models.Model):
    job = models.ForeignKey(Job, related_name='status', on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    updated_date = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=128)
    exception = models.TextField(null=True)
    output = models.TextField(null=True)

    class Meta:
        unique_together = (('job', 'status'), )

    @property
    def latest_message(self):
        try:
            latest = self.messages.latest('created_date')
        except Message.DoesNotExist:
            message = ''
        else:
            message = latest.message

        return message

    @property
    def latest_percent(self):
        try:
            latest = self.messages.latest('created_date')
        except Message.DoesNotExist:
            percent = ''
        else:
            percent = latest.percent

        return percent

    @property
    def exception_clean(self):
        removed_tag = re.sub('<\\?xml.*>\\n', '', self.exception)

        cleaned = re.sub('ExceptionReport.*>', 'ExceptionReport>', removed_tag)

        return cleaned

    def set_message(self, message, percent=None):
        self.messages.create(message=message, percent=percent)

        self.updated_date = timezone.now()

        self.save()

    def __str__(self):
        return '{0.status}'.format(self)


class Message(models.Model):
    status = models.ForeignKey(Status, related_name='messages', on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    percent = models.PositiveIntegerField(null=True)
    message = models.TextField(null=True)

    def __str__(self):
        return '{0.message}'.format(self)
