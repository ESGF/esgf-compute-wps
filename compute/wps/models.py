from __future__ import unicode_literals

import base64
import datetime
import logging
import os
import time
from urlparse import urlparse

logger = logging.getLogger('wps.models')

from cwt import wps_lib
from django.contrib.auth.models import User
from django.db import models
from django.db.models import F
from django.db.models.query_utils import Q
from django.utils import timezone
from openid import association
from openid.store import interface
from openid.store import nonce

from wps import settings
from wps import wps_xml

STATUS = {
    'ProcessAccepted': wps_lib.ProcessAccepted,
    'ProcessStarted': wps_lib.ProcessStarted,
    'ProcessPaused': wps_lib.ProcessPaused,
    'ProcessSucceeded': wps_lib.ProcessSucceeded,
    'ProcessFailed': wps_lib.ProcessFailed,
}

class DjangoOpenIDStore(interface.OpenIDStore):
    # Heavily borrowed from http://bazaar.launchpad.net/~ubuntuone-pqm-team/django-openid-auth/trunk/view/head:/django_openid_auth/store.py

    def storeAssociation(self, server_url, association):
        try:
            assoc = OpenIDAssociation.objects.get(server_url=server_url,
                                                 handle=association.handle)
        except OpenIDAssociation.DoesNotExist:
            assoc = OpenIDAssociation(server_url=server_url,
                                      handle=association.handle,
                                      secret=base64.encodestring(association.secret),
                                      issued=association.issued,
                                      lifetime=association.lifetime,
                                      assoc_type=association.assoc_type)
        else:
            assoc.secret = base64.encodestring(association.secret)
            assoc.issued = association.issued
            assoc.lifetime = association.lifetime
            assoc.assoc_type = association.assoc_type

        assoc.save()

    def getAssociation(self, server_url, handle=None):
        if handle is not None:
            assocs = OpenIDAssociation.objects.filter(server_url=server_url,
                                                     handle=handle)
        else:
            assocs = OpenIDAssociation.objects.filter(server_url=server_url)

        active = []
        expired = []

        for a in assocs:
            assoc = association.Association(a.handle,
                                            base64.decodestring(a.secret.encode('utf-8')),
                                            a.issued,
                                            a.lifetime,
                                            a.assoc_type)

            expires_in = assoc.getExpiresIn()

            if expires_in == 0:
                expired.append(a)
            else:
                active.append((assoc.issued, assoc))

        for e in expired:
            e.delete()

        if len(active) == 0:
            return None

        active.sort()

        return active[-1][1]

    def removeAssociation(self, server_url, handle):
        assocs = OpenIDAssociations.objects.filter(server_url=server_url, handle=handle)

        for a in assocs:
            a.delete()

        return len(assocs) > 0

    def useNonce(self, server_url, timestamp, salt):
        if abs(timestamp - time.time()) > nonce.SKEW:
            return False

        try:
            ononce = OpenIDNonce.objects.get(server_url__exact=server_url,
                                             timestamp__exact=timestamp,
                                             salt__exact=salt)
        except OpenIDNonce.DoesNotExist:
            ononce = OpenIDNonce.objects.create(server_url=server_url,
                                                timestamp=timestamp,
                                                salt=salt)
            return True

        return False

    def cleanupNonces(self):
        # Will implement later since it's not called
        pass

    def cleanupAssociations(self):
        # Will implement later since it's not called
        pass

class OpenIDNonce(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True)
    server_url = models.CharField(max_length=2048)
    timestamp = models.IntegerField()
    salt = models.CharField(max_length=40)

    class Meta:
        unique_together = ('server_url', 'timestamp', 'salt')

class OpenIDAssociation(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True)
    server_url = models.CharField(max_length=2048)
    handle = models.CharField(max_length=256)
    secret = models.TextField(max_length=256)
    issued = models.IntegerField()
    lifetime = models.IntegerField()
    assoc_type = models.TextField(max_length=64)

    class Meta:
        unique_together = ('server_url', 'handle')

class File(models.Model):
    name = models.CharField(max_length=256)
    host = models.CharField(max_length=256)
    variable = models.CharField(max_length=64)
    url = models.TextField()
    requested = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = ('name', 'host')

    @staticmethod
    def track(user, variable):
        url_parts = urlparse(variable.uri)

        parts = url_parts[2].split('/')

        file_obj, _ = File.objects.get_or_create(
            name=parts[-1],
            host=url_parts[1],
            variable=parts[-2],
            url=variable.uri
        )

        file_obj.requested = F('requested') + 1

        file_obj.save()

        user_file_obj, _ = UserFile.objects.get_or_create(user=user, file=file_obj)

        user_file_obj.requested = F('requested') + 1

        user_file_obj.save()

class UserFile(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    file = models.ForeignKey(File, on_delete=models.CASCADE)
    requested_date = models.DateTimeField(auto_now=True)
    requested = models.PositiveIntegerField(default=0)

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

    def __get_usage(self):
        try:
            latest = self.processusage_set.latest('created_date')
        except ProcessUsage.DoesNotExist:
            latest = self.processusage_set.create(executed=0, success=0, failed=0, retry=0)

        now = datetime.datetime.now()

        if now.month > latest.created_date.month:
            latest = self.processusage_set.create(executed=0, success=0, failed=0, retry=0)

        return latest

    def executed(self):
        usage = self.__get_usage()

        usage.executed = F('executed') + 1

        usage.save()

    def success(self):
        usage = self.__get_usage()

        usage.success = F('success') + 1

        usage.save()

    def failed(self):
        usage = self.__get_usage()

        usage.failed = F('failed') + 1

        usage.save()

    def retry(self):
        usage = self.__get_usage()

        usage.retry = F('retry') + 1

        usage.save()

    def track(self, user):
        user_process_obj, _ = UserProcess.objects.get_or_create(user=user, process=self)

        user_process_obj.requested = F('requested') + 1

        user_process_obj.save()

class UserProcess(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    process = models.ForeignKey(Process, on_delete=models.CASCADE)
    requested_date = models.DateTimeField(auto_now=True)
    requested = models.PositiveIntegerField(default=0)

class ProcessUsage(models.Model):
    process = models.ForeignKey(Process, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    executed = models.PositiveIntegerField()
    success = models.PositiveIntegerField()
    failed = models.PositiveIntegerField()
    retry = models.PositiveIntegerField()

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
        elif latest.status == 'ProcessStarted':
            message = latest.message_set.latest('created_date')

            status = STATUS.get(latest.status)(value=message.message, percent_completed=message.percent)
        else:
            status = STATUS.get(latest.status)()

        report = wps_xml.execute_response(location, status, self.process.identifier)

        if latest.output is not None:
            output = wps_xml.load_output(latest.output)

            report.add_output(output)

        return report.xml()

    def accepted(self):
        self.process.executed()

        self.status_set.create(status=wps_lib.ProcessAccepted())

    def started(self):
        status = self.status_set.create(status=str(wps_lib.ProcessStarted()).split(' ')[0])

        status.set_message('Job Started')

    def succeeded(self, output=None):
        self.process.success()

        status = self.status_set.create(status=wps_lib.ProcessSucceeded())

        if output is not None:
            status.output = wps_xml.create_output(output)

            status.save()

    def failed(self, exception=None):
        self.process.failed()

        status = self.status_set.create(status=wps_lib.ProcessFailed())

        if exception is not None:
            exc_report = wps_lib.ExceptionReport(settings.VERSION)

            exc_report.add_exception(wps_lib.NoApplicableCode, exception)

            status.exception = exc_report.xml()

            status.save()

    def retry(self):
        self.process.retry()

        self.update_process('Retrying...', 0)

    def update_progress(self, message, percent):
        started = self.status_set.filter(status='ProcessStarted').latest('created_date')

        started.set_message(message, percent)

class Status(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    updated_date = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=128)
    exception = models.TextField(null=True)
    output = models.TextField(null=True)

    def set_message(self, message, percent=None):
        self.message_set.create(message=message, percent=percent)

        self.updated_date = timezone.now()

        self.save()

class Message(models.Model):
    status = models.ForeignKey(Status, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    percent = models.PositiveIntegerField(null=True)
    message = models.TextField(null=True)
