from django.contrib.auth import authenticate
from django.contrib.auth import login
from django.http import HttpResponse
from django.shortcuts import redirect
from wps import logger

from esgf_auth.conf import settings
from esgf_auth import models

def parse_auth_header(header):
    """ Pasre HTTP Authorization header. """
    method, auth = header.split(' ')

    decoded_auth = auth.decode('base64')

    last_idx = decoded_auth.rfind(':')

    return decoded_auth[:last_idx], decoded_auth[last_idx+1:]

def esgf_login(request):
    """ Authenticate and log a user in. """
    if request.method == 'POST':
        return HttpResponse('POST authentication is not supported')

    username = request.GET.get('username', None)
    password = request.GET.get('password', None)

    if not username or not password:
        if 'HTTP_AUTHORIZATION' not in request.META:
            return HttpResponse('Expecting Authorization header.')

        username, password = parse_auth_header(request.META['HTTP_AUTHORIZATION'])

    user = authenticate(username=username, password=password)

    if user is not None:
        if user.is_active:
            login(request, user)

            if settings.MPC_SESSION_EXP:
                auth = models.MyProxyClientAuth.objects.get(user=user)
                
                request.session.set_expiry(auth.get_expiry(password))

            if 'next' in request.GET:
                next_url = request.GET['next']

                return redirect(next_url)
            else:
                return HttpResponse('Authenticated.')
        else:
            return HttpResponse('Account disabled.')
    else:
        return HttpResponse('You were not authenticated.')

