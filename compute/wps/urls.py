from django.conf import settings
from django.conf.urls import url
from django.conf.urls import include
from django.contrib import admin

import views

urlpatterns = [
    url(r'^armstrong/', include('grappelli.urls')),
    url(r'^neil/', admin.site.urls),
    url(r'^$', views.wps_entrypoint),
    url(r'^search/$', views.search_dataset),
    url(r'^search/variable/$', views.search_variable),
    url(r'^generate/$', views.generate),
    url(r'^status/(?P<job_id>[0-9]*)/$', views.status),
    url(r'^jobs/$', views.jobs),
    url(r'^jobs/(?P<job_id>[0-9]*)/$', views.job),
    url(r'^home/?', views.home, name='home'),
    url(r'^regen_capabilities/$', views.regen_capabilities),
    url(r'^metrics/?$', views.metrics_view),
    url(r'^combine/?$', views.combine),
]
