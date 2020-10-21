import re

from rest_framework import authentication
from rest_framework import exceptions

from compute_wps import models

def authenticate(meta):
    try:
        header = meta['X-Forwarded-User']
    except KeyError:
        return None

    username, _ = re.match('(.*)@(.*)', header).groups()

    user, _ = models.User.objects.get_or_create(username=username, email=header)

    return user

class TraefikAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        user = authenticate(request.META)

        if user is None:
            return user

        return (user, None)
