from django.conf import settings
from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.wps),
    url(r'^processes/$', views.processes),
    url(r'^search/$', views.search_dataset),
    url(r'^generate/$', views.generate),
    url(r'^execute/$', views.execute),
    url(r'^notification/$', views.notification),
    url(r'^status/(?P<job_id>[0-9]*)/$', views.status),
    url(r'^jobs/$', views.jobs),
    url(r'^jobs/(?P<job_id>[0-9]*)/$', views.job),
    url(r'^home/', views.home, name='home'),
    url(r'^regen_capabilities/$', views.regen_capabilities),
    url(r'^admin/stats$', views.admin_stats),
]

if settings.DEBUG:
    urlpatterns.append(url(r'^output/(?P<file_name>.*)$', views.output))
