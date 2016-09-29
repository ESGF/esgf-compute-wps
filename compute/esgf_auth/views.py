from django.contrib.auth import authenticate
from django.contrib.auth import login
from django.http import HttpResponse
from django.shortcuts import redirect

from esgf_auth.conf import settings
from esgf_auth.models import MyProxyClientAuth

from OpenSSL import crypto

from datetime import datetime

def parse_auth_header(header):
    """ Pasre HTTP Authorization header. """
    method, auth = header.split(' ')

    return auth.decode('base64').split(':')

def esgf_login(request):
    """ Authenticate and log a user in. """
    if 'HTTP_AUTHORIZATION' not in request.META:
        return HttpResponse('Expecting Authorization header.')

    username, password = parse_auth_header(request.META['HTTP_AUTHORIZATION'])

    user = authenticate(username=username, password=password)

    if user is not None:
        if user.is_active:
            login(request, user)

            if settings.MPC_SESSION_EXP:
                auth = MyProxyClientAuth.objects.get(user=user)
                
                request.session.set_expiry(auth.get_expiry(password))

            next_url = request.GET['next']

            return redirect(next_url)
        else:
            return HttpResponse('Account disabled.')
    else:
        return HttpResponse('You were not authenticated.')

