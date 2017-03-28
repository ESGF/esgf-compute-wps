from django.conf import settings
from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.wps),
    url(r'^login/$', views.oauth2_login, name='login'),
    url(r'^callback/$', views.oauth2_callback),
    url(r'^servers/$', views.servers),
    url(r'^instances/$', views.instances),
    url(r'^processes/$', views.processes),
    url(r'^jobs/$', views.jobs),
    url(r'^job/(?P<job_id>[0-9]*)/$', views.status),
]

if settings.DEBUG:
    urlpatterns += [url(r'^debug/$', views.debug)]
