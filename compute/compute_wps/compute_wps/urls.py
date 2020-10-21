from __future__ import absolute_import
from django.conf import settings
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
    url(r'^client_registration/$', auth.client_registration),
    url(r'^login/$', auth.login),
    url(r'^oauth_callback/$', auth.login_complete)
]

api_urlpatterns = [
    url(r'^', include(router.urls)),
    url(r'^ping/$', service.ping),
    url(r'^status/(?P<job_id>[0-9]*)/$', service.status),
    url(r'^metrics/$', metrics.metrics_view),
]

schema = get_schema_view(
        title='Internal API',
        patterns=internal_router.urls,
        url='{!s}/internal_api'.format(settings.INTERNAL_API_URL),
)

internal_router_urls = internal_router.urls
internal_router_urls.append(url('^schema$', schema))

urlpatterns = [
    url(r'^wps/?$', service.wps_entrypoint),
    url(r'^auth/', include(auth_urlpatterns)),
    url(r'^api/', include(api_urlpatterns)),
    url(r'^internal_api/', include(internal_router_urls)),
]
