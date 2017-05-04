from django.conf.urls import url

import views

# Should the auth views/models be split into their own app?

urlpatterns = [
    url(r'^create/$', views.create, name='create'),
    url(r'^user/$', views.user, name='user'),
    url(r'^login/$', views.login, name='login'),
    url(r'^logout/$', views.logout_view, name='logout'),
    url(r'^login/oauth2/$', views.login_oauth2, name='oauth2'),
    url(r'^login/mpc/$', views.login_mpc, name='mpc'),
    url(r'^callback/$', views.oauth2_callback),
]
