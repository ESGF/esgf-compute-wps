from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.view_main),
    url(r'^api/processes$', views.api_processes),
    url(r'^wps/status/(?P<file_name>.*(\.xml)?)$', views.status),
    url(r'^wps/', views.wps),
]
