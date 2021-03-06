#! /usr/bin/env python

import logging

from django.conf import settings
from rest_framework import mixins
from rest_framework import renderers as default_renderers
from rest_framework import viewsets

from compute_wps import models
from compute_wps import renderers
from compute_wps import serializers

# from compute_wps import metrics

logger = logging.getLogger("compute_wps.views.job")


class ProcessViewSet(mixins.CreateModelMixin, viewsets.GenericViewSet):
    queryset = models.Process.objects.all()
    serializer_class = serializers.ProcessSerializer


class MessageViewSet(mixins.CreateModelMixin, viewsets.GenericViewSet):
    queryset = models.Message.objects.all()
    serializer_class = serializers.MessageSerializer


class StatusViewSet(
    mixins.RetrieveModelMixin,
    mixins.CreateModelMixin,
    viewsets.GenericViewSet,
):
    queryset = models.Status.objects.all()
    serializer_class = serializers.StatusSerializer

    renderer_classes = [
        default_renderers.JSONRenderer,
        renderers.WPSRenderer,
    ]


class JobViewSet(
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    mixins.DestroyModelMixin,
    viewsets.GenericViewSet,
):
    queryset = models.Job.objects.all()
    serializer_class = serializers.JobSerializer

    renderer_classes = [
        default_renderers.JSONRenderer,
        renderers.WPSRenderer,
    ]

    def get_queryset(self):
        user = self.request.user

        if settings.AUTH_TYPE == "noauth":
            return models.Job.objects.all()

        return models.Job.objects.filter(user=user)


class OutputViewSet(mixins.CreateModelMixin, viewsets.GenericViewSet):
    queryset = models.Output.objects.all()
    serializer_class = serializers.OutputSerializer
