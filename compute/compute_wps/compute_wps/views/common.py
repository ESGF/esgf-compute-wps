import logging

from django import http

from compute_wps.exceptions import WPSError

logger = logging.getLogger('compute_wps.views')


class AuthenticationError(WPSError):
    def __init__(self, user):
        msg = 'Authentication for "{username}" failed'

        if isinstance(user, str):
            username = user
        else:
            username = user.username

        super(AuthenticationError, self).__init__(msg, username=username)


class AuthorizationError(WPSError):
    def __init__(self, user):
        msg = 'Authorization "{username}" failed'

        super(AuthorizationError, self).__init__(msg, username=user.username)


class MissingParameterError(WPSError):
    def __init__(self, name):
        msg = 'Parameter "{name}" is missing'

        super(MissingParameterError, self).__init__(msg, name=name)


class FormError(WPSError):
    def __init__(self, errors):
        msg = 'Form is invalid: {errors}'

        super(FormError, self).__init__(msg, errors=errors)


class UserDoesNotExistError(WPSError):
    def __init__(self, username):
        msg = 'User "{username}" does not exist'

        super(UserDoesNotExistError, self).__init__(msg, username=username)


class UserEmailDoesNotExistError(WPSError):
    def __init__(self, email):
        msg = 'User with email "{email}" does not exist'

        super(UserEmailDoesNotExistError, self).__init__(msg, email=email)


class DuplicateUserError(WPSError):
    def __init__(self, username):
        msg = 'Duplicate user "{username}"'

        super(DuplicateUserError, self).__init__(msg, username=username)


class MailError(WPSError):
    def __init__(self, email):
        msg = 'Sending mail to "{email}" failed'

        super(MailError, self).__init__(msg, email=email)


def validate_form(form, keys):
    if not form.is_valid():
        raise FormError(form.errors)

    for key in keys:
        if key not in form.cleaned_data:
            raise MissingParameterError(name=key)

    return form.cleaned_data


def user_to_json(user):
    data = {
        'username': user.username,
        'email': user.email,
        'openid': user.auth.openid_url,
        'type': user.auth.type,
        'api_key': user.auth.api_key,
        'admin': user.is_superuser,
        'local_init': not (user.password == '')
    }

    return data


def authentication_required(request):
    if not request.user.is_authenticated:
        raise AuthenticationError(request.user)


def authorization_required(request):
    if not request.user.is_superuser:
        raise AuthorizationError(request.user)


def success(data=None):
    response = {
        'status': 'success',
        'data': data
    }

    return http.JsonResponse(response)


def failed(error=None):
    response = {
        'status': 'failed',
        'error': error
    }

    return http.JsonResponse(response)
