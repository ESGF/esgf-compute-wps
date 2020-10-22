import re

from rest_framework import authentication

from compute_wps import models
from compute_wps import exceptions

def authenticate(meta):
    try:
        header = meta['X-Forwarded-User']
    except KeyError:
        raise exceptions.AuthError()

    username, _ = re.match('(.*)@(.*)', header).groups()

    user, _ = models.User.objects.get_or_create(username=username, email=header)

    return user

class TraefikAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        try:
            user = authenticate(request.META)
        except exceptions.AuthError:
            return None

        if user is None:
            return None

        return (user, None)
