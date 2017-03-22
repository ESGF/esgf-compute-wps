from django.conf import settings
from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.wps),
    url(r'^servers/$', views.servers),
    url(r'^job/(?P<job_id>[0-9]*)/$', views.status),
]

if settings.DEBUG:
    urlpatterns += [url(r'^debug/$', views.debug)]
