from __future__ import division
from __future__ import unicode_literals

from builtins import object
import logging
import os

from django.contrib.auth.models import User
from django.db import IntegrityError
from django.db import models
from django.db.models.signals import post_delete
from django.dispatch import receiver

from compute_wps.util import wps_response

logger = logging.getLogger("compute_wps.models")


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
        unique_together = ("name", "host", "variable")

    def __str__(self):
        return f"Url {self.url} variable {self.variable}"


class UserFile(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    file = models.ForeignKey(File, on_delete=models.CASCADE)
    requested_date = models.DateTimeField(auto_now=True)
    requested = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f"User {self.user.pk} file {self.file.url}"


class Process(models.Model):
    identifier = models.CharField(max_length=128, blank=False)
    abstract = models.TextField()
    metadata = models.TextField()
    version = models.CharField(max_length=128, blank=False, default=None)

    class Meta(object):
        unique_together = (("identifier", "version"),)

    def __str__(self):
        return f"Process {self.identifier}"


class UserProcess(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    process = models.ForeignKey(Process, on_delete=models.CASCADE)
    requested_date = models.DateTimeField(auto_now=True)
    requested = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f"User {self.user.pk} process {self.process.identifier}"


class Job(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    process = models.ForeignKey(Process, on_delete=models.CASCADE)
    datainputs = models.TextField()

    def __str__(self):
        return f"User {self.user.pk} process {self.process.identifier}"

    def accepted(self):
        status = self.status_set.create(status=wps_response.ProcessAccepted)

        return status.id

    def failed(self, exc):
        if not isinstance(exc, str):
            exc = str(exc)

        try:
            status = self.status_set.create(status=wps_response.ProcessFailed)
            status.message_set.create(message=exc)
        except IntegrityError:
            logger.error("Failed status already exists")

    def status(self):
        latest = self.status_set.latest('created_date')

        return latest.status

    def elapsed(self):
        try:
            earliest = self.status_set.earliest('created_date')

            latest = self.status_set.latest('updated_date')
        except Exception:
            return None

        return latest.updated_date-earliest.created_date

    def accepted_on(self):
        status = self.status_set.get(status='ProcessAccepted')

        return status.created_date

class Status(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    updated_date = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=128)

    class Meta:
        unique_together = (("job", "status"),)

    def __str__(self):
        return f"Job {self.job.pk} status {self.status}"


class Message(models.Model):
    status = models.ForeignKey(Status, on_delete=models.CASCADE)

    created_date = models.DateTimeField(auto_now_add=True)
    percent = models.PositiveIntegerField(null=True)
    message = models.TextField(null=True)

    def __str__(self):
        return (
            f"Job {self.status.job.pk} message {self.message} percent"
            f" {self.percent}"
        )


class Output(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)

    local = models.CharField(max_length=256)
    remote = models.CharField(max_length=512)
    size = models.DecimalField(max_digits=14, decimal_places=4)

    def __str__(self):
        return f"local {self.local} remote {self.remote} size {self.size}"


@receiver(post_delete, sender=Output)
def post_delete_output_callback(sender, instance, **kwargs):
    logger.info(f"Removing {instance.local!r}")

    if os.path.exists(instance.local):
        os.remove(instance.local)
