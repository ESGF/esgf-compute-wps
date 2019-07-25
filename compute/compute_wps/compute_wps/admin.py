from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User
from django.contrib.sessions.models import Session

from compute_wps import models


class AuthInline(admin.TabularInline):
    model = models.Auth


class UserAdmin(UserAdmin):
    inlines = (AuthInline,)


admin.site.unregister(User)
admin.site.register(User, UserAdmin)


@admin.register(Session)
class SessionAdmin(admin.ModelAdmin):
    def _session_data(self, obj):
        return obj.get_decoded()

    pass


@admin.register(models.OpenIDNonce)
class OpenIDNonceAdmin(admin.ModelAdmin):
    pass


@admin.register(models.OpenIDAssociation)
class OpenIDAssociationAdmin(admin.ModelAdmin):
    pass


@admin.register(models.File)
class FileAdmin(admin.ModelAdmin):
    pass


@admin.register(models.Process)
class ProcessAdmin(admin.ModelAdmin):
    pass


@admin.register(models.Server)
class ServerAdmin(admin.ModelAdmin):
    pass


class OutputInline(admin.TabularInline):
    model = models.Output


class StatusInline(admin.TabularInline):
    model = models.Status


@admin.register(models.Job)
class JobAdmin(admin.ModelAdmin):
    inlines = (OutputInline, StatusInline)


class MessageInline(admin.TabularInline):
    model = models.Message


@admin.register(models.Status)
class StatusAdmin(admin.ModelAdmin):
    inlines = (MessageInline,)
