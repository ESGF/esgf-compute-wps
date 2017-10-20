import logging

from django import http

logger = logging.getLogger('wps.views')

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
