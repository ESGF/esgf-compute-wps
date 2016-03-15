from django.conf.urls import patterns, include, url
from django.contrib import admin
import views
import os
print os.path.join(os.path.dirname(__file__),"..","test_urls.html"),
urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'wps.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^admin/', include(admin.site.urls)),
    url(r'^login/$', 'django.contrib.auth.views.login'),
    url(r'^wps/', views.wps),
    url(r'^postprocess/', views.postprocess),
    url(r'^status/',views.status),
    url(r'^clear/(\d+)',views.clear_process),
    url(r'^view/(\d+)',views.view_process),
    url(r'^$', views.view_main),
)
