from builtins import object
from rest_framework import serializers
from rest_framework.reverse import reverse

from wps import models
from wps.util import wps_response


class UserFileSerializer(serializers.ModelSerializer):
    url = serializers.URLField(write_only=True)
    var_name = serializers.CharField(write_only=True)

    class Meta(object):
        model = models.UserFile
        fields = ('id', 'user', 'file', 'requested_date', 'requested', 'url', 'var_name')
        read_only_fields = ('user', 'file')

    def create(self, validated_data):
        validated_data.pop('url')

        validated_data.pop('var_name')

        user_file = models.UserFile.objects.create(**validated_data)

        return user_file


class UserProcessSerializer(serializers.ModelSerializer):
    class Meta(object):
        model = models.UserProcess
        fields = ('id', 'user', 'process', 'requested_date', 'requested')
        read_only_fields = ('user', 'process')


class ProcessSerializer(serializers.ModelSerializer):
    abstract = serializers.CharField(required=False, default='')
    metadata = serializers.CharField(required=False, default='{}')
    version = serializers.CharField()

    class Meta(object):
        model = models.Process
        fields = ('id', 'identifier', 'backend', 'abstract', 'metadata', 'version')


class MessageSerializer(serializers.ModelSerializer):
    message = serializers.CharField()
    percent = serializers.FloatField(required=False, default=0)

    class Meta(object):
        model = models.Message
        fields = ('id', 'created_date', 'message', 'percent')


class StatusSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)
    messages = MessageSerializer(many=True, read_only=True)
    status = serializers.ChoiceField(choices=['ProcessAccepted', 'ProcessStarted', 'ProcessPaused',
                                              'ProcessSucceeded', 'ProcessFailed'], required=True)

    class Meta(object):
        model = models.Status
        fields = ('id', 'status', 'created_date', 'messages', 'output', 'exception')

    def create(self, validated_data):
        if 'exception' in validated_data:
            validated_data['exception'] = wps_response.exception_report(wps_response.NoApplicableCode,
                                                                        validated_data['exception'])

        return models.Status.objects.create(**validated_data)


class StatusHyperlink(serializers.HyperlinkedRelatedField):
    view_name = 'status-detail'
    queryset = models.Status.objects.all()

    def get_url(self, obj, view_name, request, format):
        # Construct the url from the related job pk and the status pk
        url_kwargs = {
            'job_pk': obj.job.pk,
            'pk': obj.pk
        }

        return reverse(view_name, kwargs=url_kwargs, request=request, format=format)


class JobSerializer(serializers.ModelSerializer):
    server = serializers.SlugRelatedField(read_only=True, slug_field='host')

    process = serializers.SlugRelatedField(read_only=True, slug_field='identifier')

    elapsed = serializers.ReadOnlyField()

    latest_status = serializers.ReadOnlyField()

    accepted_on = serializers.ReadOnlyField()

    status = StatusHyperlink(many=True)

    class Meta(object):
        model = models.Job
        fields = ('id', 'server', 'process', 'extra', 'elapsed', 'latest_status', 'accepted_on', 'status')
