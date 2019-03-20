#! /usr/bin/env python

from rest_framework import mixins
from rest_framework import permissions
from rest_framework import viewsets

from wps import models
from wps import serializers

class StatusViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = models.Status.objects.all()
    serializer_class = serializers.StatusSerializer

    def get_queryset(self):
        user = self.request.user

        job_pk = self.kwargs['job_pk']

        return models.Status.objects.filter(job__pk=job_pk, 
                                            job__user=user.pk)

class JobViewSet(mixins.ListModelMixin, 
                 mixins.RetrieveModelMixin, 
                 mixins.DestroyModelMixin, 
                 viewsets.GenericViewSet):
    queryset = models.Job.objects.all()
    serializer_class = serializers.JobSerializer

    def get_queryset(self):
        user = self.request.user

        return models.Job.objects.filter(user=user.pk)
