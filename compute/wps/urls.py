from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.index), 
    url(r'^wps', views.wps),
    url(r'^wps/status/(?P<file_name>.*(\.xml)?)$', views.status),
    url(r'^api/processes$', views.api_processes),
]
