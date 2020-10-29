import os
import json
import logging

import cwt
from builtins import object
from django.conf import settings
from rest_framework import serializers
from rest_framework.reverse import reverse

from compute_wps import metrics
from compute_wps import models
from compute_wps.util import wps_response

logger = logging.getLogger('compute_wps.serializers')


class ProcessSerializer(serializers.ModelSerializer):
    abstract = serializers.CharField(required=False, default='')
    metadata = serializers.CharField(required=False, default='{}')
    version = serializers.CharField()

    class Meta(object):
        model = models.Process
        fields = ('id', 'identifier', 'abstract', 'metadata', 'version')


class MessageSerializer(serializers.ModelSerializer):
    message = serializers.CharField()
    percent = serializers.FloatField(required=False, default=0)

    class Meta(object):
        model = models.Message
        fields = '__all__'


class StatusSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)
    status = serializers.ChoiceField(choices=wps_response.StatusChoices, required=True)
    message = MessageSerializer(many=True, read_only=True, source='message_set')

    class Meta(object):
        model = models.Status
        fields = '__all__'

class JobSerializer(serializers.ModelSerializer):
    identifier = serializers.SlugRelatedField(
        read_only=True,
        slug_field='identifier',
        source='process')

    status = serializers.HyperlinkedRelatedField(
        read_only=True,
        many=True,
        view_name='status-detail',
        source='status_set')

    class Meta(object):
        model = models.Job
        exclude = ('user', 'process')
