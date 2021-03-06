from builtins import object
import logging

from rest_framework import serializers

from compute_wps import models
from compute_wps.util import wps_response

logger = logging.getLogger("compute_wps.serializers")


class ProcessSerializer(serializers.ModelSerializer):
    abstract = serializers.CharField(required=False, default="")
    metadata = serializers.CharField(required=False, default="{}")
    version = serializers.CharField()

    class Meta(object):
        model = models.Process
        fields = ("id", "identifier", "abstract", "metadata", "version")


class MessageSerializer(serializers.ModelSerializer):
    message = serializers.CharField()
    percent = serializers.FloatField(required=False, default=0)

    class Meta(object):
        model = models.Message
        fields = "__all__"


class StatusSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)
    status = serializers.ChoiceField(
        choices=wps_response.StatusChoices, required=True
    )
    message = MessageSerializer(
        many=True, read_only=True, source="message_set"
    )

    class Meta(object):
        model = models.Status
        fields = "__all__"


class OutputSerializer(serializers.ModelSerializer):
    job = serializers.PrimaryKeyRelatedField(
        write_only=True,
        queryset=models.Job.objects,
    )
    local = serializers.CharField(write_only=True)

    class Meta(object):
        model = models.Output
        fields = "__all__"


class JobSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.IntegerField()
    identifier = serializers.SlugRelatedField(
        read_only=True, slug_field="identifier", source="process"
    )

    status = serializers.DateTimeField()

    status_links = serializers.HyperlinkedRelatedField(
        read_only=True,
        many=True,
        view_name="status-detail",
        source="status_set",
    )

    elapsed = serializers.DurationField()

    output = OutputSerializer(many=True, read_only=True, source="output_set")

    accepted = serializers.DateTimeField(source="accepted_on")

    class Meta(object):
        model = models.Job
        exclude = ("user", "process")
