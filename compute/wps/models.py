from __future__ import unicode_literals

from django.contrib.auth.models import User
from django.db import models

class OAuth2(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)

    openid = models.CharField(max_length=256)
    token_type = models.CharField(max_length=64)
    refresh_token = models.CharField(max_length=64)
    access_token = models.CharField(max_length=64)
    scope = models.CharField(max_length=128)
    expires_at = models.DateField()
    api_key = models.CharField(max_length=256)

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
    identifier = models.CharField(max_length=128)
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

class Status(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)

    status = models.IntegerField()
    created_date = models.DateTimeField(auto_now_add=True)
    result = models.TextField()
