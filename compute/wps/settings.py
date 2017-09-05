#! /usr/bin/env python

from functools import partial

from django.conf import settings

setting = partial(getattr, settings)

# General Settings
OAUTH2_CALLBACK = setting('WPS_OAUTH2_CALLBACK', 'https://aims2.llnl.gov/auth/callback')

HOSTNAME = setting('WPS_EXTERNAL_HOSTNAME', '0.0.0.0')
PORT = setting('WPS_EXTERNAL_PORT', '8000')

CACHE_PATH = setting('WPS_CACHE_PATH', '/data/cache')

ENDPOINT = setting('WPS_ENDPOINT', 'https://aims2.llnl.gov/wps')
STATUS_LOCATION = setting('WPS_STATUS_LOCATION', 'https://aims2.llnl.gov/wps/job/{job_id}')

OUTPUT_LOCAL_PATH = setting('WPS_OUTPUT_LOCAL_PATH', '/data/public')
OUTPUT_URL = setting('WPS_OUTPUT_URL', 'http://0.0.0.0:8000/wps/output/{file_name}')

DAP = setting('WPS_DAP', True)
DAP_URL = setting('WPS_DAP_URL', 'https://aims2.llnl.gov/thredds/dodsC/test/public/{file_name}')

CA_PATH = setting('WPS_CA_PATH', '/tmp/certs')

ADMIN_EMAIL = setting('WPS_ADMIN_EMAIL', 'admin@wps.llnl.gov')

LOGIN_URL = setting('WPS_LOGIN_URL', 'http://0.0.0.0:8000/wps/home/login')

OPENID_TRUST_ROOT = setting('WPS_OPENID_TRUST_ROOT', 'http://0.0.0.0:8000/wps/home/login/openid')
OPENID_RETURN_TO = setting('WPS_OPENID_RETURN_TO', 'http://0.0.0.0:8000/auth/callback/openid')

CREATE_SUBJECT = 'Welcome to ESGF compute server'
CREATE_MESSAGE = """
Thank you for creating an account for the ESGF compute server. Please login into your account <a href="{login_url}">here</a>.

If you have any questions or concerns please email the <a href="mailto:{admin_email}">server admin</a>.
"""

# WPS Settings
VERSION = setting('WPS_VERSION', '1.0.0')
SERVICE = setting('WPS_SERVICE', 'WPS')
LANG = setting('WPS_LANG', 'en-US')
TITLE = setting('WPS_TITLE', 'LLNL WPS')
NAME = setting('WPS_NAME', 'Lawerence Livermore National Laboratory')
SITE = setting('WPS_SITE', 'https://llnl.gov')
