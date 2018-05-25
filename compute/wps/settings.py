#! /usr/bin/env python

import datetime
from functools import partial

from django.conf import settings

setting = partial(getattr, settings)

# General Settings
HOSTNAME = setting('WPS_EXTERNAL_HOSTNAME', '0.0.0.0')
PORT = setting('WPS_EXTERNAL_PORT', '8000')

ENDPOINT = setting('WPS_ENDPOINT', 'http://0.0.0.0:8000/wps')
STATUS_LOCATION = setting('WPS_STATUS_LOCATION', 'http://0.0.0.0:8000/wps/status/{job_id}')

LOCAL_OUTPUT_PATH = setting('WPS_LOCAL_OUTPUT_PATH', '/data/public')
OUTPUT_URL = setting('WPS_OUTPUT_URL', 'http://0.0.0.0:8000/wps/output/{filename}')

DAP = setting('WPS_DAP', True)
DAP_URL = setting('WPS_DAP_URL', 'http://thredds:8080/threddsCWT/dodsC/public/{filename}')

CA_PATH = setting('WPS_CA_PATH', '/tmp/certs')

ADMIN_EMAIL = setting('WPS_ADMIN_EMAIL', 'admin@aims2.llnl.gov')

LOGIN_URL = setting('WPS_LOGIN_URL', 'http://0.0.0.0:8000/wps/home/login')
PROFILE_URL = setting('WPS_PROFILE_URL', 'http://0.0.0.0:8000/wps/home/profile')

OAUTH2_CALLBACK = setting('WPS_OAUTH2_CALLBACK', 'http://0.0.0.0:8000/auth/callback')

OPENID_TRUST_ROOT = setting('WPS_OPENID_TRUST_ROOT', 'http://0.0.0.0:8000/wps/home/login/openid')
OPENID_RETURN_TO = setting('WPS_OPENID_RETURN_TO', 'http://0.0.0.0:8000/auth/callback/openid')
OPENID_CALLBACK_SUCCESS = setting('WPS_OPENID_CALLBACK_SUCCESS', 'http://0.0.0.0:8000/wps/home/login/callback')

PASSWORD_RESET_URL = setting('WPS_PASSWORD_RESET_URL', 'http://0.0.0.0:8000/wps/home/login/reset')

USER_TEMP_PATH = setting('WPS_USER_TEMP_PATH', '/tmp')

PARTITION_SIZE = setting('WPS_PARTITION_SIZE', 10)

# Cache settings
CACHE_PATH = setting('WPS_CACHE_PATH', '/data/cache')
# Default to checking cache once a day
CACHE_CHECK = setting('WPS_CACHE_CHECK', datetime.timedelta(days=1))
# Default to 100 GB
CACHE_GB_MAX_SIZE = setting('WPS_CACHE_MAX_GB_SIZE', 2.097152e8)
# Default to 30 days
CACHE_MAX_AGE = setting('WPS_CACHE_MAX_AGE', datetime.timedelta(days=30))
CACHE_FREED_PERCENT = setting('WPS_CACHE_FREED_PERCENT', 0.25)

# EDAS Settings
EDAS_HOST = setting('WPS_EDAS_HOST', 'edas')
EDAS_TIMEOUT = setting('WPS_EDAS_TIMEOUT', 30)
EDAS_REQ_PORT = setting('WPS_EDAS_REQ_PORT', 5670)
EDAS_RES_PORT = setting('WPS_EDAS_RES_PORT', 5671)

# Ophidia Settings
OPH_USER = setting('WPS_OPHIDIA_USER', 'oph-test')
OPH_PASSWORD = setting('WPS_OPHIDIA_PASSWORD', 'abcd')
OPH_HOST = setting('WPS_OPHIDIA_HOST', 'aims2.llnl.gov')
OPH_PORT = setting('WPS_OPHIDIA_PORT', '11732')
OPH_OUTPUT_PATH = setting('WPS_OPHIDIA_OUTPUT_PATH', '/wps')
OPH_OUTPUT_URL = setting('WPS_OPHIDIA_OUTPUT_URL', 'https://aims2.llnl.gov/thredds/dodsC{output_path}/{output_name}.nc')
OPH_DEFAULT_CORES = setting('WPS_OPHIDIA_DEFAULT_CORES', 2)

# WPS Settings
VERSION = setting('WPS_VERSION', '1.0.0')
SERVICE = setting('WPS_SERVICE', 'WPS')
LANG = setting('WPS_LANG', 'en-US')
TITLE = setting('WPS_TITLE', 'LLNL WPS')
NAME = setting('WPS_NAME', 'Lawerence Livermore National Laboratory')
SITE = setting('WPS_SITE', 'https://llnl.gov')
