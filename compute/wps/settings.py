#! /usr/bin/env python

from django.conf import settings

OAUTH2_CALLBACK = getattr(settings, 'WPS_OAUTH2_CALLBACK', 'https://aims2.llnl.gov/auth/callback')

HOSTNAME = getattr(settings, 'WPS_EXTERNAL_HOSTNAME', 'aims2.llnl.gov')
PORT = getattr(settings, 'WPS_EXTERNAL_PORT', None)
