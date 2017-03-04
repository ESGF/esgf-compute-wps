from __future__ import unicode_literals

from django.db import models

class Process(models.Model):
    identifier = models.CharField(max_length=128)
    description = models.TextField()

class Instance(models.Model):
    host = models.CharField(max_length=128, unique=True, blank=False, null=False)
    added = models.DateTimeField(auto_now_add=True)
    request = models.PositiveIntegerField(default=4356)
    response = models.PositiveIntegerField(default=4357)
    status = models.IntegerField(default=1)
    checked = models.DateTimeField(auto_now=True)
    queue = models.PositiveIntegerField(default=0)
    queue_size = models.PositiveIntegerField(default=0)

class Server(models.Model):
    host = models.CharField(max_length=128)
    added = models.DateTimeField(auto_now_add=True)
    status = models.IntegerField(default=1)
    capabilities = models.TextField()
    processes = models.ManyToManyField(Process)

class Job(models.Model):
    server = models.ForeignKey(Server, on_delete=models.CASCADE)
    result = models.TextField()

class JobState(models.Model):
    state = models.IntegerField()
    create = models.DateTimeField(auto_now_add=True)
    job = models.ForeignKey(Job, on_delete=models.CASCADE)


