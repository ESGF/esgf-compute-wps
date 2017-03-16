from __future__ import unicode_literals

from django.db import models

class Instance(models.Model):
    host = models.CharField(max_length=128, unique=True, blank=False, null=False)
    added_date = models.DateTimeField(auto_now_add=True)
    request = models.PositiveIntegerField(default=4356)
    response = models.PositiveIntegerField(default=4357)
    status = models.IntegerField(default=1)
    checked_date = models.DateTimeField(auto_now=True)
    queue = models.PositiveIntegerField(default=0)
    queue_size = models.PositiveIntegerField(default=0)

class Server(models.Model):
    host = models.CharField(max_length=128)
    added_date = models.DateTimeField(auto_now_add=True)
    status = models.IntegerField(default=1)
    capabilities = models.TextField()

class Job(models.Model):
    server = models.ForeignKey(Server, on_delete=models.CASCADE)

class Status(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)

    status = models.IntegerField()
    created_date = models.DateTimeField(auto_now_add=True)
    result = models.TextField()
