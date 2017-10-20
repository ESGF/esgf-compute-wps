from django.conf import settings
from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.wps),
    url(r'^processes/$', views.processes),
    url(r'^search/$', views.search_esgf),
    url(r'^generate/$', views.generate),
    url(r'^execute/$', views.execute),
    url(r'^notification/$', views.notification),
    url(r'^stats/files/$', views.stats_files),
    url(r'^stats/processes/$', views.stats_processes),
    url(r'^job/(?P<job_id>[0-9]*)/$', views.status),
    url(r'^jobs/$', views.jobs),
    url(r'^jobs/(?P<job_id>[0-9]*)/$', views.job),
    url(r'^home/', views.home, name='home'),
    url(r'^regen_capabilities/$', views.regen_capabilities),
]

if settings.DEBUG:
    urlpatterns.append(url(r'^output/(?P<file_name>.*)$', views.output))
