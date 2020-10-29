#! /usr/bin/env python

import logging
from urllib.parse import urlparse

from django import db
from django.db.models import Count
from django.db.models import F
from django.db.models import Max
from django.db.models import Min
from rest_framework import filters
from rest_framework import mixins
from rest_framework import viewsets
from rest_framework import permissions
from rest_framework.authentication import BasicAuthentication
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import DjangoModelPermissions
from rest_framework.exceptions import APIException
from rest_framework import renderers as default_renderers

# from compute_wps import metrics
from compute_wps import models
from compute_wps import serializers
from compute_wps import renderers

logger = logging.getLogger('compute_wps.views.job')


class ProcessViewSet(mixins.CreateModelMixin,
                     viewsets.GenericViewSet):
    queryset = models.Process.objects.all()
    serializer_class = serializers.ProcessSerializer

class MessageViewSet(mixins.CreateModelMixin,
                     viewsets.GenericViewSet):
    queryset = models.Message.objects.all()
    serializer_class = serializers.MessageSerializer


class StatusViewSet(mixins.RetrieveModelMixin,
                    mixins.CreateModelMixin,
                    viewsets.GenericViewSet):
    queryset = models.Status.objects.all()
    serializer_class = serializers.StatusSerializer


class JobViewSet(mixins.ListModelMixin,
                 mixins.RetrieveModelMixin,
                 mixins.DestroyModelMixin,
                 viewsets.GenericViewSet):
    queryset = models.Job.objects.all()
    serializer_class = serializers.JobSerializer

    serializer_classes = {
        'list': serializers.JobSerializer,
        'retrieve': serializers.JobDetailSerializer,
    }

    renderer_classes = [
        default_renderers.JSONRenderer,
        renderers.WPSStatusRenderer,
    ]

    def get_serializer_class(self):
        return self.serializer_classes.get(self.action)

    def get_queryset(self):
        user = self.request.user

        return models.Job.objects.filter(
            user=user)
