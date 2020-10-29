from __future__ import absolute_import
from django.conf import settings
from django.urls import re_path
from django.conf.urls import include
from django.contrib import admin
from rest_framework import renderers
from rest_framework.authentication import BasicAuthentication
from rest_framework.schemas import get_schema_view
from rest_framework import routers
from rest_framework.urlpatterns import format_suffix_patterns

from compute_wps.views import auth
from compute_wps.views import job
from compute_wps.views import metrics
from compute_wps.views import service

router = routers.SimpleRouter()
router.register('message', job.MessageViewSet)
router.register('status', job.StatusViewSet)
router.register('process', job.ProcessViewSet)

job_router = routers.SimpleRouter()
job_router.register('job', job.JobViewSet)

# Only require the format on job-detail view
router_urls = router.urls
router_urls.append(job_router.urls[0])
router_urls.extend(
    format_suffix_patterns(
        job_router.urls[1:2], allowed=['json', 'wps']))

schema = get_schema_view(
    title='WPS API',
    patterns=router.urls,
    url=f'{settings.BASE_URL}/api')

urlpatterns = [
    re_path('^wps/?$', service.wps_entrypoint),
    re_path('^api/', include(router_urls)),
    re_path('^api/schema', schema),
    re_path('^api/ping/$', service.ping),
    re_path('^api/metrics/$', metrics.metrics_view),
    re_path('^auth/client_registration/$', auth.client_registration),
    re_path('^auth/login/$', auth.login),
    re_path('^auth/oauth_callback/$', auth.login_complete),
]

if settings.DEBUG:
    from django.conf.urls.static import static

    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
