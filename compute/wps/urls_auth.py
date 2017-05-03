from django.conf.urls import url

import views

# Should the auth views/models be split into their own app?

urlpatterns = [
    url(r'^login/$', views.login, name='login'),
    url(r'^create/$', views.create, name='create'),
    url(r'^login/oauth2/$', views.login_oauth2, name='oauth2'),
    url(r'^login/mpc/$', views.login_mpc, name='mpc'),
    url(r'^callback/$', views.oauth2_callback),
]
