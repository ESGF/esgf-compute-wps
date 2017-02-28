from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.wps),
    url(r'^/status/(?P<job_id>[0-9]*)/$', views.status),
]
