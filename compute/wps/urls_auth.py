from django.conf.urls import url

import views

# Should the auth views/models be split into their own app?

urlpatterns = [
    url(r'^update/$', views.update),
    url(r'^authorization/?$', views.authorization),
    url(r'^user/$', views.user_details),
    url(r'^user/cert/$', views.user_cert),
    url(r'^user/stats/$', views.user_stats),
    url(r'^user/regenerate/$', views.regenerate),
    url(r'^login/openid/$', views.user_login_openid),
    url(r'^logout/$', views.user_logout),
    url(r'^login/oauth2/$', views.login_oauth2),
    url(r'^login/mpc/$', views.login_mpc),
    url(r'^callback$', views.oauth2_callback),
    url(r'^callback/openid/$', views.user_login_openid_callback),
]
