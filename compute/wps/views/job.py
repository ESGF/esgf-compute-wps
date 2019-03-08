#! /usr/bin/env python

from rest_framework import mixins
from rest_framework import viewsets

from wps import models
from wps import serializers

class StatusViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = models.Status.objects.all()
    serializer_class = serializers.StatusSerializer

class JobViewSet(mixins.ListModelMixin, 
                 mixins.RetrieveModelMixin, 
                 mixins.DestroyModelMixin, 
                 viewsets.GenericViewSet):
    queryset = models.Job.objects.all()
    serializer_class = serializers.JobSerializer
