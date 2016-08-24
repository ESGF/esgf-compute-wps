from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.view_main),
    url(r'^wps/', views.wps),
]
