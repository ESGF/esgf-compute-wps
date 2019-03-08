from rest_framework import serializers

from wps import models

class MessageSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Message
        fields = ('id', 'created_date', 'message', 'percent')

class StatusSerializer(serializers.ModelSerializer):
    messages = MessageSerializer(many=True)

    class Meta:
        model = models.Status
        fields = ('id', 'status', 'created_date', 'messages')

class JobSerializer(serializers.ModelSerializer):
    server = serializers.SlugRelatedField(read_only=True,
                                          slug_field='host')

    process = serializers.SlugRelatedField(read_only=True,
                                           slug_field='identifier')

    elapsed = serializers.ReadOnlyField()

    latest_status = serializers.ReadOnlyField()

    accepted_on = serializers.ReadOnlyField()

    status = serializers.HyperlinkedRelatedField(
        many=True,
        read_only=True,
        view_name='status-detail'
    )

    class Meta:
        model = models.Job
        fields = ('id', 'server', 'process', 'extra', 'elapsed',
                  'latest_status', 'accepted_on', 'status')
