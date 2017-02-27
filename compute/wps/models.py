from __future__ import unicode_literals

from django.db import models

class Server(models.Model):
    name = models.CharField(max_length=256, blank=True, null=True)
    address = models.CharField(max_length=64, unique=True)
    request = models.IntegerField(default=4356)
    response = models.IntegerField(default=4357)
    is_wps = models.BooleanField(default=False)
    queue = models.PositiveIntegerField(default=0)
    queue_size = models.PositiveIntegerField(default=0)

class Job(models.Model):
    server = models.ForeignKey(Server, on_delete=models.CASCADE)
    status = models.CharField(max_length=64)
    result = models.TextField()
