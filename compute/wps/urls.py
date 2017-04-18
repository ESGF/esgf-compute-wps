from django.conf import settings
from django.conf.urls import url

import views

urlpatterns = [
               url(r'^$', views.wps),
               url(r'^job/(?P<job_id>[0-9]*)/$', views.status),
              ]

if settings.DEBUG:
    urlpatterns.extend([
                        # API endpoints
                        url(r'^api/servers/$', views.servers),
                        url(r'^api/instances/$', views.instances),
                        url(r'^api/processes/$', views.processes),
                        url(r'^api/jobs/$', views.jobs),

                        url(r'^debug/$', views.debug),

                        url(r'^regen_capabilities/$', views.regen_capabilities),

                        url(r'^output/(?P<file_name>.*)$', views.output),
                       ])
