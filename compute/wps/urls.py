from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.view_main),
    url(r'^wps/', views.wps),
    url(r'^status/',views.status),
    url(r'^clear/(\d+)',views.clear_process),
    url(r'^view/(\d+)',views.view_process),
]
