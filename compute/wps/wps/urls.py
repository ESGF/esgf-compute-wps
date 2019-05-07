from __future__ import absolute_import
from django.conf.urls import url
from django.conf.urls import include
from django.contrib import admin
from rest_framework.authentication import BasicAuthentication
from rest_framework.schemas import get_schema_view
from rest_framework.routers import SimpleRouter

from . import views

router = SimpleRouter()
router.register(r'jobs', views.JobViewSet)
router.register(r'jobs/(?P<job_pk>[^/.]+)/status', views.StatusViewSet)

internal_router = SimpleRouter()
internal_router.register(r'status/(?P<status>[^/.]+)', views.InternalStatusViewSet, basename='internal')
internal_router.register(r'jobs/(?P<job_pk>[^/.]+)/status', views.InternalStatusViewSet, basename='internal')
internal_router.register(r'jobs/(?P<job_pk>[^/.]+)/status/(?P<status_pk>[^/.]+)/message', views.InternalMessageViewSet,
                         basename='internal')
internal_router.register(r'process', views.InternalProcessViewSet, basename='internal')
internal_router.register(r'user/(?P<user_pk>[^/.]+)/file', views.InternalUserFileViewSet, basename='internal')
internal_router.register(r'user/(?P<user_pk>[^/.]+)/process/(?P<process_pk>[^/.]+)', views.InternalUserProcessViewSet,
                         basename='internal')

api_urlpatterns = [
    url(r'^', include(router.urls)),

    url(r'^armstrong/', include('grappelli.urls')),
    url(r'^neil/', admin.site.urls),

    url(r'^ping/$', views.ping),
    url(r'^search/$', views.search_dataset),
    url(r'^search/variable/$', views.search_variable),
    url(r'^status/(?P<job_id>[0-9]*)/$', views.status),
    url(r'^metrics/$', views.metrics_view),
    url(r'^combine/$', views.combine),

    # Authentication and authorization
    url(r'^user/$', views.user_details),
    url(r'^user/authorization/$', views.authorization),
    url(r'^user/cert/$', views.user_cert),
    url(r'^user/regenerate/$', views.regenerate),
    url(r'^user/stats/$', views.user_stats),
    url(r'^user/update/$', views.update),
    url(r'^openid/login/$', views.user_login_openid),
    url(r'^openid/logout/$', views.user_logout),
    url(r'^openid/callback/$', views.user_login_openid_callback),
    url(r'^oauth2/$', views.login_oauth2),
    url(r'^oauth2/callback/$', views.oauth2_callback),
    url(r'^mpc/$', views.login_mpc),
]

schema = get_schema_view(title='Internal API', patterns=internal_router.urls, url='https://10.5.5.5/internal_api',
                         authentication_classes=[BasicAuthentication, ])

internal_router_urls = internal_router.urls
internal_router.urls.append(url('^schema$', schema))

urlpatterns = [
    url(r'^wps/$', views.wps_entrypoint),
    url(r'^api/', include(api_urlpatterns)),
    url(r'^internal_api/', include(internal_router_urls)),
]
