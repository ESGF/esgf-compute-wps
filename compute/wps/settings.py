#! /usr/bin/env python

from django.conf import settings

OAUTH2_CALLBACK = getattr(settings, 'WPS_OAUTH2_CALLBACK', 'https://aims2.llnl.gov/auth/callback')

HOSTNAME = getattr(settings, 'WPS_EXTERNAL_HOSTNAME', 'aims2.llnl.gov')
PORT = getattr(settings, 'WPS_EXTERNAL_PORT', None)

OUTPUT_LOCAL_PATH = getattr(settings, 'WPS_OUTPUT_LOCAL_PATH', '/data')
OUTPUT_URL = getattr(settings, 'WPS_OUTPUT_URL', 'http://0.0.0.0:8000/wps/output/{file_name}')

DAP = getattr(settings, 'WPS_DAP', False)
DAP_URL = getattr(settings, 'WPS_DAP_URL', 'https://aims2.llnl.gov/{file_name}')
