from __future__ import unicode_literals

import os
import logging

logger = logging.getLogger('wps.models')

from cwt import wps_lib
from django.contrib.auth.models import User
from django.db import models
from django.db.models.query_utils import Q

from wps import settings
from wps import wps_xml

STATUS = {
    'ProcessAccepted': wps_lib.ProcessAccepted,
    'ProcessStarted': wps_lib.ProcessStarted,
    'ProcessPaused': wps_lib.ProcessPaused,
    'ProcessSucceeded': wps_lib.ProcessSucceeded,
    'ProcessFailed': wps_lib.ProcessFailed,
}

class Cache(models.Model):
    uid = models.CharField(max_length=256)

    url = models.CharField(max_length=512)
    dimensions = models.TextField()
    added_date = models.DateTimeField(auto_now_add=True)
    accessed_date = models.DateTimeField(auto_now=True)
    size = models.PositiveIntegerField(null=True)

    @property
    def local_path(self):
        file_name = '{}.nc'.format(self.uid)

        return os.path.join(settings.CACHE_PATH, file_name)

class Auth(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)

    openid = models.TextField()
    openid_url = models.CharField(max_length=256)
    type = models.CharField(max_length=64)
    cert = models.TextField()
    api_key = models.CharField(max_length=128)
    extra = models.TextField()

class Instance(models.Model):
    host = models.CharField(max_length=128, unique=True, blank=False, null=False)
    added_date = models.DateTimeField(auto_now_add=True)
    request = models.PositiveIntegerField(default=4356)
    response = models.PositiveIntegerField(default=4357)
    status = models.IntegerField(default=1)
    checked_date = models.DateTimeField(auto_now=True)
    queue = models.PositiveIntegerField(default=0)
    queue_size = models.PositiveIntegerField(default=0)

class Process(models.Model):
    identifier = models.CharField(max_length=128, unique=True)
    backend = models.CharField(max_length=128)
    description = models.TextField()

class Server(models.Model):
    host = models.CharField(max_length=128)
    added_date = models.DateTimeField(auto_now_add=True)
    status = models.IntegerField(default=1)
    capabilities = models.TextField()

    processes = models.ManyToManyField(Process)

class Job(models.Model):
    server = models.ForeignKey(Server, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    process = models.ForeignKey(Process, on_delete=models.CASCADE, null=True)
    extra = models.TextField(null=True)

    @property
    def status(self):
        return [
            {
                'create_date':  x.created_date,
                'status': x.status,
                'exception': x.exception,
                'output': x.output
            } for x in self.status_set.all().order_by('created_date')
        ]

    @property
    def elapsed(self):
        started = self.status_set.filter(status='ProcessStarted')

        ended = self.status_set.filter(Q(status='ProcessSucceeded') | Q(status='ProcessFailed'))

        if len(started) == 0 or len(ended) == 0:
            return 'No elapsed'

        elapsed = ended[0].created_date - started[0].created_date

        return '{}.{}'.format(elapsed.seconds, elapsed.microseconds)

    @property
    def report(self):
        location = settings.STATUS_LOCATION.format(job_id=self.id)

        latest = self.status_set.latest('created_date')

        if latest.exception is not None:
            exc_report = wps_lib.ExceptionReport.from_xml(latest.exception)

            status = STATUS.get(latest.status)(exception_report=exc_report)
        else:
            status = STATUS.get(latest.status)()

        report = wps_xml.execute_response(location, status, self.process.identifier)

        if latest.output is not None:
            output = wps_xml.load_output(latest.output)

            report.add_output(output)

        return report.xml()

    def accepted(self):
        self.status_set.create(status=wps_lib.ProcessAccepted())

    def started(self):
        status = self.status_set.create(status=wps_lib.ProcessStarted())

        status.set_message('Job Started')

    def succeeded(self, output=None):
        status = self.status_set.create(status=wps_lib.ProcessSucceeded())

        if output is not None:
            status.output = wps_xml.create_output(output)

            status.save()

    def failed(self, exception=None):
        status = self.status_set.create(status=wps_lib.ProcessFailed())

        if exception is not None:
            exc_report = wps_lib.ExceptionReport(settings.VERSION)

            exc_report.add_exception(wps_lib.NoApplicableCode, exception)

            status.exception = exc_report.xml()

            status.save()

    def update_progress(self, message, percent):
        started = self.status_set.filter(status='ProcessStarted').latest('created_date')

        started.set_message(message, percent)

class Status(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=128)
    exception = models.TextField(null=True)
    output = models.TextField(null=True)

    def set_message(self, message, percent=None):
        self.message_set.create(message=message, percent=percent)

class Message(models.Model):
    status = models.ForeignKey(Status, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    percent = models.PositiveIntegerField(null=True)
    message = models.TextField(null=True)
