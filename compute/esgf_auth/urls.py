from django.conf.urls import url

from views import esgf_login

urlpatterns = [
    url(r'^login/$', esgf_login, name='login'),
]
