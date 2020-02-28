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

from compute_wps import metrics
from compute_wps import models
from compute_wps import serializers
from compute_wps.auth import token_authentication

logger = logging.getLogger('compute_wps.views.job')


class InternalFileViewSet(viewsets.GenericViewSet):
    queryset = models.File.objects.all()
    serializer_class = serializers.FileSerializer
    authentication_classes = (BasicAuthentication, )
    permission_classes = (DjangoModelPermissions, )

    @action(detail=False)
    def distinct_users(self, request):
        query = models.File.objects.all().annotate(Count('userfile', distinct=True)).values(
            'url', 'userfile__count', 'requested')

        data = dict((x['url'], {'count': x['requested'], 'unique_users': x['userfile__count']}) for x in query)

        return Response(data, status=200)


class InternalStatusViewSet(viewsets.GenericViewSet):
    queryset = models.Status.objects.all()
    serializer_class = serializers.StatusSerializer
    authentication_classes = (BasicAuthentication, )
    permission_classes = (DjangoModelPermissions, )

    @action(detail=False)
    def unique_count(self, request):
        # Get the status for each job that was last updated
        query = models.Status.objects.values('job').annotate(Max('updated_date')).values('status')

        # Get the number of times each status is present
        query = query.annotate(Count('status'))

        data = dict((x['status'], x['status__count']) for x in query)

        return Response(data, status=200)


class InternalUserFileViewSet(mixins.CreateModelMixin,
                              viewsets.GenericViewSet):
    queryset = models.UserFile.objects.all()
    serializer_class = serializers.UserFileSerializer
    authentication_classes = (BasicAuthentication, )
    permission_classes = (DjangoModelPermissions, )

    def create(self, request, *args, **kwargs):
        try:
            user = models.User.objects.get(pk=kwargs['user_pk'])
        except models.User.DoesNotExist:
            return Response('User does not exist', status=400)

        parts = urlparse(request.data['url'])

        path_parts = parts.path.split('/')

        file, created = models.File.objects.get_or_create(name=path_parts[-1], host=parts.netloc)

        fields = ['requested', ]

        if created:
            file.url = request.data['url']

            file.variable = request.data['var_name']

            fields.extend(['url', 'variable'])

        file.requested = F('requested') + 1

        file.save(update_fields=fields)

        user_file, _ = models.UserFile.objects.get_or_create(user=user, file=file)

        user_file.requested = F('requested') + 1

        user_file.save(update_fields=['requested'])

        user_file.refresh_from_db()

        user_file_serializer = serializers.UserFileSerializer(instance=user_file)

        return Response(user_file_serializer.data, status=201)


class InternalUserProcessViewSet(mixins.CreateModelMixin,
                                 viewsets.GenericViewSet):
    queryset = models.UserProcess.objects.all()
    serializer_class = serializers.UserProcessSerializer
    authentication_classes = (BasicAuthentication, )
    permission_classes = (DjangoModelPermissions, )

    def create(self, request, *args, **kwargs):
        try:
            user = models.User.objects.get(pk=kwargs['user_pk'])
        except models.User.DoesNotExist:
            raise APIException('User does not exist')

        try:
            process = models.Process.objects.get(pk=kwargs['process_pk'])
        except models.Process.DoesNotExist:
            raise APIException('Process does not exist')

        user_process, _ = models.UserProcess.objects.get_or_create(user=user, process=process)

        user_process.requested = F('requested') + 1

        user_process.save(update_fields=['requested'])

        user_process.refresh_from_db()

        user_process_serializer = serializers.UserProcessSerializer(instance=user_process)

        return Response(user_process_serializer.data, status=201)


class InternalProcessViewSet(mixins.CreateModelMixin,
                             mixins.ListModelMixin,
                             viewsets.GenericViewSet):
    queryset = models.Process.objects.all()
    serializer_class = serializers.ProcessSerializer
    authentication_classes = (BasicAuthentication, )
    permission_classes = (DjangoModelPermissions, )


class InternalJobMessageViewSet(mixins.CreateModelMixin,
                                viewsets.GenericViewSet):
    queryset = models.Message.objects.all()
    serializer_class = serializers.MessageSerializer
    authentication_classes = (BasicAuthentication, )
    permission_classes = (DjangoModelPermissions, )

    def create(self, request, *args, **kwargs):
        try:
            status = models.Status.objects.get(job__pk=kwargs['job_pk'], pk=kwargs['status_pk'])
        except models.Status.DoesNotExist:
            return Response('Status does not exist', status=400)

        message_serializer = serializers.MessageSerializer(data=request.data)

        message_serializer.is_valid(raise_exception=True)

        message_serializer.save(status=status)

        return Response(message_serializer.data, status=201)


class InternalJobViewSet(viewsets.GenericViewSet):
    queryset = models.Job.objects.all()
    serializer_class = serializers.JobSerializer
    authentication_classes = (BasicAuthentication, )
    permission_classes = (DjangoModelPermissions, )

    @action(detail=True)
    def set_output(self, request, pk):
        try:
            job = models.Job.objects.get(pk=pk)
        except models.Job.DoesNotExist:
            raise APIException('Job does not exist')

        job.output.create(path=request.query_params['path'])

        return Response(status=201)


class InternalJobStatusViewSet(mixins.CreateModelMixin,
                               viewsets.GenericViewSet):
    queryset = models.Status.objects.all()
    serializer_class = serializers.StatusSerializer
    authentication_classes = (BasicAuthentication, )
    permission_classes = (DjangoModelPermissions, )

    def create(self, request, *args, **kwargs):
        try:
            job = models.Job.objects.get(pk=kwargs['job_pk'])
        except models.Job.DoesNotExist:
            raise APIException('Job does not exist')

        status_serializer = serializers.StatusSerializer(data=request.data)

        status_serializer.is_valid(raise_exception=True)

        try:
            status_serializer.save(job=job)
        except db.IntegrityError:
            raise APIException('Status {!r} already exists for job {!r}'.format(request.data['status'], job.id))

        if request.data['status'] == 'ProcessStarted':
            metrics.WPS_JOBS_STARTED.inc()
        elif request.data['status'] == 'ProcessSucceeded':
            metrics.WPS_JOBS_SUCCEEDED.inc()
        elif request.data['status'] == 'ProcessFailed':
            metrics.WPS_JOBS_FAILED.inc()

        return Response(status_serializer.data, status=201)


class StatusViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = models.Status.objects.all()
    serializer_class = serializers.StatusSerializer
    authentication_classes = (token_authentication.TokenAuthentication, )
    permission_classes = (permissions.IsAuthenticated, )

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
    filter_backends = (filters.OrderingFilter, )
    ordering_fields = ('accepted', )
    authentication_classes = (token_authentication.TokenAuthentication, )
    permission_classes = (permissions.IsAuthenticated, )

    @action(detail=False, methods=['delete'])
    def remove_all(self, request):
        user = request.user

        models.Job.objects.filter(user=user.pk).delete()

        return Response(status=204)

    def get_queryset(self):
        user = self.request.user

        return models.Job.objects.filter(user=user.pk).annotate(accepted=Min('status__created_date'))
