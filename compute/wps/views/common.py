import collections
import logging

from django import http

logger = logging.getLogger('wps.views')

class ViewError(Exception):
    def __init__(self, message):
        self.message = message

class MissingParameterError(ViewError):
    FMT = 'Missing parameter "{parameter}"'

    def __init__(self, **kwargs):
        super(MissingParameterError, self).__init__(self.FMT.format(**kwargs))

class DuplicateUserError(ViewError):
    FMT = 'User "{username}" already exists'

    def __init__(self, **kwargs):
        super(DuplicateUserError, self).__init__(self.FMT.format(**kwargs))

def validate_form(form, keys):
    if not form.is_valid():    
        raise ViewError(form.errors)

    for key in keys:
        if key not in form.cleaned_data:
            raise MissingParameterError(parameter=key)

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
        raise Exception('Unauthorized access')

def authorization_required(request):
    if not request.user.is_superuser:
        raise Exception('Forbidden access')

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
