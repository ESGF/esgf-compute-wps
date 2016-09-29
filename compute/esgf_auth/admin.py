from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User

from esgf_auth.models import MyProxyClientAuth

class MyProxyClientAuthInline(admin.StackedInline):
    model = MyProxyClientAuth
    can_delete = False
    verbose_name = 'MyProxyClient'
    verbose_name_plural = 'MyProxyClient'

class UserAdmin(UserAdmin):
    inlines = (MyProxyClientAuthInline,)

admin.site.unregister(User)
admin.site.register(User, UserAdmin)
