from django.conf.urls import url

import views

# Should the auth views/models be split into their own app?

urlpatterns = [
    url(r'^login/$', views.oauth2_login, name='login'),
    url(r'^callback/$', views.oauth2_callback),
]
