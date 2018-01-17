from __future__ import unicode_literals

import base64
import datetime
import hashlib
import json
import logging
import os
import random
import string
import time
from urlparse import urlparse

import cdms2
from cwt import wps_lib
from django.contrib.auth.models import User
from django.db import models
from django.db.models import F
from django.db.models import signals
from django.db.models.query_utils import Q
from django.utils import timezone
from openid import association
from openid.store import interface
from openid.store import nonce

from wps import settings
from wps import wps_xml

logger = logging.getLogger('wps.models')

KBYTE = 1024
MBYTE = KBYTE * KBYTE
GBYTE = MBYTE * KBYTE

STATUS = {
    'ProcessAccepted': wps_lib.ProcessAccepted,
    'ProcessStarted': wps_lib.ProcessStarted,
    'ProcessPaused': wps_lib.ProcessPaused,
    'ProcessSucceeded': wps_lib.ProcessSucceeded,
    'ProcessFailed': wps_lib.ProcessFailed,
}

class Notification(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    message = models.TextField()
    created_date = models.DateTimeField(auto_now_add=True)
    enabled = models.BooleanField(default=True)

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
            variable=variable.var_name,
            url=variable.uri
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

def slice_default(obj):
    if isinstance(obj, slice):
        return {'slice': '{}:{}:{}'.format(obj.start, obj.stop, obj.step)}

    return json.JSONEncoder.default(obj)

def slice_object_hook(obj):
    if 'slice' not in obj:
        return obj

    data = obj['slice'].split(':')

    start = int(data[0])

    stop = int(data[1])

    if data[2] == 'None':
        step = None
    else:
        step = int(data[2])

    return slice(start, stop, step)

class Cache(models.Model):
    uid = models.CharField(max_length=256)
    url = models.CharField(max_length=513)
    dimensions = models.TextField()
    added_date = models.DateTimeField(auto_now_add=True)
    accessed_date = models.DateTimeField(null=True)
    size = models.DecimalField(null=True, max_digits=16, decimal_places=8)

    @property
    def local_path(self):
        dimension_hash = hashlib.sha256(self.uid+self.dimensions).hexdigest()

        file_name = '{}.nc'.format(dimension_hash)

        return os.path.join(settings.CACHE_PATH, file_name)

    @property
    def valid(self):
        if not os.path.exists(self.local_path):
            logger.info('Cache file does not exist on disk')

            return False

        data = json.loads(self.dimensions, object_hook=slice_object_hook)

        logger.info('Checking domain "{}"'.format(data))

        try:
            var_name = data['variable']

            temporal = data['temporal']

            spatial = data['spatial']
        except KeyError as e:
            logger.info('Missing key "{}" in dimensions'.format(e))

            return False

        with cdms2.open(self.local_path) as infile:
            if var_name not in infile.variables:
                logger.info('Cache file does not contain variable "{}"'.format(var_name))

                return False

            temporal_axis = infile[var_name].getTime()

            if temporal_axis is None:
                logger.info('Cache file is missing a temporal axis')

                return False

            if not self.__axis_valid(temporal_axis, temporal):
                logger.info('Cache files temporal axis is invalid')

                return False

            for name, spatial_def in spatial.items():
                spatial_axis_index = infile[var_name].getAxisIndex(name)

                if spatial_axis_index == -1:
                    logger.info('Cache file is missing spatial axis "{}"'.format(name))

                    return False

                spatial_axis = infile[var_name].getAxis(spatial_axis_index)

                if not self.__axis_valid(spatial_axis, spatial_def):
                    logger.info('Cache files spatial axis "{}" is invalid'.format(name))

                    return False

        return True

    def __axis_valid(self, axis_data, axis_def):
        axis_size = axis_data.shape[0]

        if isinstance(axis_def, slice):
            expected_size = axis_def.stop - axis_def.start
        else:
            logger.info('Axis definition is unknown')

            return False

        logger.info('Cache files axis actual size {}, expected size {}'.format(axis_size, expected_size))

        return axis_size == expected_size

    def __cmp_axis(self, value, cached):
        logger.info('Axis {} cached {}'.format(value, cached))

        if (value is not None and cached is None or
                value is None and cached is None):
            return True

        if ((value is None and cached is not None) or
                value.start < cached.start or
                value.stop > cached.stop):
            return False

        return  True

    def is_superset(self, domain):
        temporal = domain['temporal']

        spatial = domain['spatial']

        try:
            cached = json.loads(self.dimensions, object_hook=slice_object_hook)
        except ValueError:
            return False

        logger.info('Checking if "{}" is a superset of "{}"'.format(cached, domain))

        cached_temporal = cached['temporal']

        if not self.__cmp_axis(temporal, cached_temporal):
            logger.info('Temporal axis is does not match')

            return False

        cached_spatial = cached['spatial']

        for name, cached_spatial_axis in cached_spatial.items():
            if name == 'variable':
                continue

            if not self.__cmp_axis(spatial.get(name), cached_spatial_axis):
                logger.info('Spatial axis "{}" does not match'.format(name))

                return False

        return True

    def accessed(self, when=None):
        if when is None:
            when = timezone.now()

        self.accessed = when

        self.save()

    def estimate_size(self):
        with cdms2.open(self.url) as infile:
            var_name, dimensions = self.dimensions.split('!')

            size = reduce(lambda x, y: x * y, infile[var_name].shape)

            size = size * infile[var_name].dtype.itemsize

        size = size / GBYTE

        return size

def cache_clean_files(sender, **kwargs):
    cache = kwargs['instance']

    if os.path.exists(cache.local_path):
        os.remove(cache.local_path)

signals.post_delete.connect(cache_clean_files, sender=Cache)

class Auth(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)

    openid = models.TextField()
    openid_url = models.CharField(max_length=256)
    type = models.CharField(max_length=64)
    cert = models.TextField()
    api_key = models.CharField(max_length=128)
    extra = models.TextField()

    def update(self, auth_type, certs, **kwargs):
        if self.api_key == '':
            self.api_key = ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(64))

        self.type = auth_type

        self.cert = ''.join(certs)

        extra = json.loads(self.extra or '{}')

        extra.update(kwargs)

        self.extra = json.dumps(extra)

        self.save()

class Process(models.Model):
    identifier = models.CharField(max_length=128, unique=True)
    backend = models.CharField(max_length=128)
    abstract = models.TextField()
    description = models.TextField()
    enabled = models.BooleanField(default=True)

    def get_usage(self, rollover=True):
        try:
            latest = self.processusage_set.latest('created_date')
        except ProcessUsage.DoesNotExist:
            latest = self.processusage_set.create(executed=0, success=0, failed=0, retry=0)

        if rollover:
            now = datetime.datetime.now()

            if now.month > latest.created_date.month:
                latest = self.processusage_set.create(executed=0, success=0, failed=0, retry=0)

        return latest

    def executed(self):
        usage = self.get_usage()

        usage.executed = F('executed') + 1

        usage.save()

    def success(self):
        usage = self.get_usage()

        usage.success = F('success') + 1

        usage.save()

    def failed(self):
        usage = self.get_usage()

        usage.failed = F('failed') + 1

        usage.save()

    def retry(self):
        usage = self.get_usage()

        usage.retry = F('retry') + 1

        usage.save()

    def track(self, user):
        user_process_obj, _ = UserProcess.objects.get_or_create(user=user, process=self)

        user_process_obj.requested = F('requested') + 1

        user_process_obj.save()

    def to_json(self, usage=False):
        data = {
            'identifier': self.identifier,
            'backend': self.backend,
            'enabled': self.enabled
        }

        if usage:
            try:
                usage_obj = self.processusage_set.latest('created_date')
            except ProcessUsage.DoesNotExist:
                data.update({
                    'executed': None,
                    'success': None,
                    'failed': None,
                    'retry': None
                })
            else:
                data.update(usage_obj.to_json())

        return data

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

class ProcessUsage(models.Model):
    process = models.ForeignKey(Process, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    executed = models.PositiveIntegerField()
    success = models.PositiveIntegerField()
    failed = models.PositiveIntegerField()
    retry = models.PositiveIntegerField()

    def to_json(self):
        return {
            'executed': self.executed,
            'success': self.success,
            'failed': self.failed,
            'retry': self.retry
        }

class Server(models.Model):
    host = models.CharField(max_length=128, unique=True)
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
    def details(self):
        return {
            'id': self.id,
            'server': self.server.host,
            'process': self.process.identifier,
            'elapsed': self.elapsed,
            #'status': self.status
        }

    @property
    def status(self):
        return [
            {
                'created_date': x.created_date,
                'updated_date': x.updated_date,
                'status': x.status,
                'exception': x.exception,
                'output': x.output,
                'messages': [y.details for y in x.message_set.all().order_by('created_date')]
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

    def retry(self, exception):
        self.process.retry()

        self.update_status('Retrying... {}'.format(exception), 0)

    def update_status(self, message, percent=0):
        started = self.status_set.filter(status='ProcessStarted').latest('created_date')

        started.set_message(message, percent)

    def statusSince(self, date):
        return [
            {
                'created_date': x.created_date,
                'updated_date': x.updated_date,
                'status': x.status,
                'exception': x.exception,
                'output': x.output,
                'messages': [y.details for y in x.message_set.all().order_by('created_date')]
            } for x in self.status_set.filter(updated_date__gt=date)
        ]

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

    @property
    def details(self):
        return {
            'created_date': self.created_date,
            'percent': self.percent or 0,
            'message': self.message
        }
