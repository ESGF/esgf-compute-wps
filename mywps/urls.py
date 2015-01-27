from django.conf.urls import patterns, include, url
from django.contrib import admin
import mywps.views

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'mywps.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^admin/', include(admin.site.urls)),
    url(r'^login/$', 'django.contrib.auth.views.login'),
    url(r'^wps/', mywps.views.wps),
    url(r'^status/',mywps.views.status),
)
