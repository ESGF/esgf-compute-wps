from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.view_main),
    url(r'^api/processes', views.api_processes),
    url(r'^wps/', views.wps),
]
