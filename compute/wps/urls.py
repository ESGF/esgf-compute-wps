from django.conf import settings
from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.wps),
    url(r'^servers/$', views.servers),
    url(r'^instances/$', views.instances),
    url(r'^processes/$', views.processes),
    url(r'^jobs/$', views.jobs),
    url(r'^job/(?P<job_id>[0-9]*)/$', views.status),
]

if settings.DEBUG:
    urlpatterns += [url(r'^debug/$', views.debug)]

    urlpatterns += [url(r'^output/(?P<file_name>.*)$', views.output)]
