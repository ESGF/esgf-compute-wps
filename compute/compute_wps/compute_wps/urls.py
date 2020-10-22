from __future__ import absolute_import
from django.conf import settings
from django.urls import re_path
from django.conf.urls import url
from django.conf.urls import include
from django.contrib import admin
from rest_framework import renderers
from rest_framework.authentication import BasicAuthentication
from rest_framework.schemas import get_schema_view
from rest_framework.routers import SimpleRouter

from compute_wps.views import auth
from compute_wps.views import job
from compute_wps.views import metrics
from compute_wps.views import service

router = SimpleRouter()
router.register(r'jobs', job.JobViewSet)
router.register(r'jobs/(?P<job_pk>[^/.]+)/status', job.StatusViewSet)

internal_router = SimpleRouter()
internal_router.register(r'files', job.InternalFileViewSet, basename='internal')
internal_router.register(r'status', job.InternalStatusViewSet, basename='internal')
internal_router.register(r'process', job.InternalProcessViewSet, basename='internal')
internal_router.register(r'jobs', job.InternalJobViewSet, basename='internal')
internal_router.register(
    r'jobs/(?P<job_pk>[^/.]+)/status',
    job.InternalJobStatusViewSet,
    basename='internal')
internal_router.register(
    r'jobs/(?P<job_pk>[^/.]+)/status/(?P<status_pk>[^/.]+)/message',
    job.InternalJobMessageViewSet,
    basename='internal')
internal_router.register(
    r'user/(?P<user_pk>[^/.]+)/file',
    job.InternalUserFileViewSet,
    basename='internal')
internal_router.register(
    r'user/(?P<user_pk>[^/.]+)/process/(?P<process_pk>[^/.]+)',
    job.InternalUserProcessViewSet,
    basename='internal')

auth_urlpatterns = [
    re_path(r'^client_registration/$', auth.client_registration),
    re_path(r'^login/$', auth.login),
    re_path(r'^oauth_callback/$', auth.login_complete)
]

api_urlpatterns = [
    re_path(r'^', include(router.urls)),
    re_path(r'^ping/$', service.ping),
    re_path(r'^status/(?P<job_id>[0-9]*)/$', service.status),
    re_path(r'^metrics/$', metrics.metrics_view),
]

schema = get_schema_view(
        title='Internal API',
        patterns=internal_router.urls,
        url='{!s}/internal_api'.format(settings.INTERNAL_API_URL),
)

internal_router_urls = internal_router.urls
internal_router_urls.append(re_path('^schema$', schema))

urlpatterns = [
    re_path(r'^wps/?$', service.wps_entrypoint),
    re_path(r'^auth/', include(auth_urlpatterns)),
    re_path(r'^api/', include(api_urlpatterns)),
    re_path(r'^internal_api/', include(internal_router_urls)),
]
