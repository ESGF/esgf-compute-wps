from django.conf import settings
from django.conf.urls import url

import views

urlpatterns = [
               url(r'^$', views.wps),
               url(r'^job/(?P<job_id>[0-9]*)/$', views.status),
               url(r'^jobs/(?P<user_id>[0-9]*)$', views.jobs),
               url(r'^servers/$', views.servers),
               url(r'^servers/(?P<server_id>[0-9]*)$', views.processes),
               url(r'^home/', views.home, name='home'),
               url(r'^output/(?P<file_name>.*)$', views.output),
               url(r'^regen_capabilities/$', views.regen_capabilities),
              ]
