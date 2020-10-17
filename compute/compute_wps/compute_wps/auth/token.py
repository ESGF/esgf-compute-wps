import logging

from rest_framework import authentication
from rest_framework import exceptions

from compute_wps import models

logger = logging.getLogger("compute_wps.auth.token")

def authenticate(meta):
    try:
        header = meta["HTTP_COMPUTE_TOKEN"]
    except KeyError:
        return None

    logger.info("Found token header")

    try:
        user = models.User.objects.get(auth__api_key=header)
    except models.User.DoesNotExist:
        raise exceptions.AuthenticationFailed("Could not authenticate user")

    logger.info("Authenticated token user")

    return user

class TokenAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        user = authenticate(request.META)

        if user is None:
            return user

        return (user, None)
