#! /usr/bin/env python

from django.conf import settings

OAUTH2_CALLBACK = getattr(settings, 'WPS_OAUTH2_CALLBACK', 'https://aims2.llnl.gov/auth/callback')

HOSTNAME = getattr(settings, 'WPS_EXTERNAL_HOSTNAME', 'aims2.llnl.gov')
PORT = getattr(settings, 'WPS_EXTERNAL_PORT', None)

OUTPUT_LOCAL_PATH = getattr(settings, 'WPS_OUTPUT_LOCAL_PATH', '/data')
OUTPUT_URL = getattr(settings, 'WPS_OUTPUT_URL', 'http://0.0.0.0:8000/static/{file_name}')

DAP = getattr(settings, 'WPS_DAP', False)
DAP_PROTO = getattr(settings, 'WPS_DAP_PROTO', 'http')
DAP_HOST = getattr(settings, 'WPS_DAP_HOST', 'aims2.llnl.gov')
DAP_PORT = getattr(settings, 'WPS_DAP_PORT', '80')
DAP_URL_PATH = getattr(settings, 'WPS_DAP_URL_PATH', '/{file_name}')
