from __future__ import unicode_literals

from cwt import wps_lib
from django.contrib.auth.models import User
from django.db import models

from wps import settings
from wps import wps_xml

class Auth(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)

    openid = models.TextField()
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
    parent = models.ForeignKey('self', on_delete=models.CASCADE, null=True)

    report = models.TextField()

    @property
    def latest(self):
        latest = self.status_set.all().latest('created_date')

        status = wps_xml.update_execute_response_status(self.report, latest())

        return status

    def set_report(self, identifier):
        location = settings.STATUS_LOCATION.format(job_id=self.id)

        report = wps_xml.execute_response(location, wps_lib.started, identifier)

        self.report = report.xml()

        self.save()

        status = self.status_set.create(status=wps_lib.started)

        status.set_message('Job Started')

    def update_progress(self, message, percent):
        status = self.status_set.all().latest('created_date')

        status.message_set.create(message=message, percent=percent)

    def update_report_cdas(self, response):
        report = wps_xml.update_execute_cdas2_response(self.report, response)

        self.report = report.xml()

        self.status_set.create(status=report.status)

    def succeeded(self, output=None):
        if output is not None:
            report = wps_xml.update_execute_response(self.report, output) 

            self.report = report.xml()

            self.save()

        self.status_set.create(status=wps_lib.succeeded)

    def failed(self, exception=None):
        status = self.status_set.create(status=wps_lib.failed)

        if exception is not None:
            status.set_exception(exception)

class Status(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=128)

    def __call__(self):
        try:
            latest = self.message_set.all().latest('created_date')
        except Message.DoesNotExist:
            latest = None

        if latest is not None:
            return wps_xml.generate_status(self.status, latest.message, latest.percent, latest.exception)
        else:
            return wps_xml.generate_status(self.status)

    def set_message(self, message):
        self.message_set.create(message=message)

    def set_exception(self, exception):
        if isinstance(exception, (str, unicode)):
            exc_report = wps_lib.ExceptionReport(settings.VERSION)

            exc_report.add_exception(wps_lib.NoApplicableCode, exception)

            self.message_set.create(exception=exc_report.xml()) 
        else:
            self.message_set.create(exception=exception.xml())

class Message(models.Model):
    status = models.ForeignKey(Status, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    percent = models.PositiveIntegerField(null=True)
    message = models.TextField(null=True)
    exception = models.TextField(null=True)
