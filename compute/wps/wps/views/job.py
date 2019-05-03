#! /usr/bin/env python

from urllib.parse import urlparse

from django.db.models import F
from rest_framework import mixins
from rest_framework import viewsets
from rest_framework.response import Response

from wps import models
from wps import serializers


class InternalUserFileViewSet(mixins.CreateModelMixin,
                              viewsets.GenericViewSet):
    queryset = models.UserFile.objects.all()
    serializer_class = serializers.UserFileSerializer

    def create(self, *args, **kwargs):
        try:
            url = self.request.data['url']

            var_name = self.request.data['var_name']
        except KeyError as e:
            return Response('Missing required parameter {!s}'.format(e), status=400)

        try:
            user = models.User.objects.get(pk=kwargs['user_pk'])
        except models.User.DoesNotExist:
            return Response('User does not exist')

        url_parts = urlparse(url)

        parts = url_parts[2].split('/')

        file, _ = models.File.objects.get_or_create(name=parts[-1], host=url_parts.netloc,
                                                    defaults={
                                                        'variable': var_name,
                                                        'url': url,
                                                    })

        file.requested = F('requested') + 1

        file.save(update_fields=['requested'])

        user_file, _ = models.UserFile.objects.get_or_create(user=user, file=file)

        user_file.requested = F('requested') + 1

        user_file.save(update_fields=['requested'])

        user_file, _ = models.UserFile.objects.get_or_create(user=user, file=file)

        serializer = serializers.UserFileSerializer(user_file)

        return Response(serializer.data, status=201)


class InternalUserProcessViewSet(mixins.CreateModelMixin,
                                 viewsets.GenericViewSet):
    queryset = models.UserProcess.objects.all()
    serializer_class = serializers.UserProcessSerializer

    def create(self, *args, **kwargs):
        try:
            process_pk = self.request.data['process']
        except KeyError as e:
            return Response('Missing required parameter {!s}'.format(e), status=400)

        try:
            user = models.User.objects.get(pk=kwargs['user_pk'])
        except models.User.DoesNotExist:
            return Response('User does not exist')

        user_process, _ = models.UserProcess.objects.get_or_create(user=user, process=process_pk)

        user_process.requested = F('requested') + 1

        user_process.save(update_fields=['requested'])

        # After the save the requested field is not a value so we requery so serializer can handle correctly
        user_process, _ = models.UserProcess.objects.get_or_create(user=user, process=process_pk)

        serializer = serializers.UserProcessSerializer(user_process)

        return Response(serializer.data, status=201)


class InternalProcessViewSet(mixins.CreateModelMixin,
                             viewsets.GenericViewSet):
    queryset = models.Process.objects.all()
    serializer_class = serializers.ProcessSerializer

    def create(self, *args, **kwargs):
        try:
            identifier = self.request.data['identifier']

            backend = self.request.data['backend']

            version = self.request.data['version']
        except KeyError as e:
            return Response('Missing required parameter {!s}'.format(e), status=400)

        process, created = models.Process.objects.get_or_create(identifier=identifier, version=version)

        if not created:
            response = Response('Process already exists', status=400)
        else:
            process.backend = backend

            process.abstract = self.request.data.get('abstract', '')

            process.metadata = self.request.data.get('metadata', {})

            process.save(update_fields=['backend', 'abstract', 'metadata'])

            serializer = serializers.ProcessSerializer(process)

            response = Response(serializer.data, status=201)

        return response


class InternalMessageViewSet(mixins.CreateModelMixin,
                             viewsets.GenericViewSet):
    queryset = models.Message.objects.all()
    serializer_class = serializers.MessageSerializer

    def create(self, *args, **kwargs):
        percent = self.request.data.get('percent') or 0

        message_text = self.request.data.get('message')

        if message_text is None:
            response = Response('Missing required parameter "message"', status=400)
        else:
            status = models.Status.objects.get(job__pk=kwargs['job_pk'], pk=kwargs['status_pk'])

            message = status.messages.create(message=message_text, percent=percent)

            serializer = serializers.MessageSerializer(message)

            response = Response(serializer.data, status=200)

        return response


class InternalStatusViewSet(mixins.CreateModelMixin,
                            mixins.UpdateModelMixin,
                            viewsets.GenericViewSet):
    queryset = models.Status.objects.all()
    serializer_class = serializers.StatusSerializer

    def create(self, *args, **kwargs):
        status_text = self.request.data.get('status')

        if status_text is None:
            response = Response('Missing required parameter "status"', status=400)
        else:
            status, created = models.Status.objects.get_or_create(job=self.kwargs['job_pk'], status=status_text)

            if not created:
                response = Response('Status already exists', status=400)
            else:
                serializer = serializers.StatusSerializer(status)

                response = Response(serializer.data, status=201)

        return response

    def update(self, *args, **kwargs):
        try:
            status = models.Status.objects.get(job__pk=kwargs['job_pk'], pk=kwargs['pk'])
        except models.Status.DoesNotExist:
            return Response('Status does not exist', status=400)

        exception = self.request.data.get('exception')

        output = self.request.data.get('output')

        if exception is not None and output is not None:
            return Response('Can only update exception or output, not both', status=400)
        elif exception is not None:
            status.exception = exception

            status.save(update_fields=['exception'])
        elif output is not None:
            status.output = output

            status.save(update_fields=['output'])

        serializer = serializers.StatusSerializer(status)

        return Response(serializer.data, status=200)


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
