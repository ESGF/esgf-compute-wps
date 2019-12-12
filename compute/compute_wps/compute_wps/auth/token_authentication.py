from django.contrib.auth.models import User
from rest_framework import authentication
from rest_framework import exceptions

TOKEN_NAMES = ('HTTP_COMPUTE_TOKEN',)

class TokenAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        token = None

        for key in TOKEN_NAMES:
            token = request.META.get(key, None)

        if token is None:
            return None

        try:
            user = User.objects.get(auth__api_key=token)
        except User.DoesNotExist:
            raise exceptions.AuthenticationFailed('No such user')

        return (user, None)
