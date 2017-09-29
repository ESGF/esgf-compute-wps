#! /usr/bin/env python

from functools import partial

from django.conf import settings

setting = partial(getattr, settings)

# General Settings
HOSTNAME = setting('WPS_EXTERNAL_HOSTNAME', '0.0.0.0')
PORT = setting('WPS_EXTERNAL_PORT', '8000')

CACHE_PATH = setting('WPS_CACHE_PATH', '/data/cache')

ENDPOINT = setting('WPS_ENDPOINT', 'http://0.0.0.0:8000/wps')
STATUS_LOCATION = setting('WPS_STATUS_LOCATION', 'http://0.0.0.0:8000/wps/job/{job_id}')

OUTPUT_LOCAL_PATH = setting('WPS_OUTPUT_LOCAL_PATH', '/data/public')
OUTPUT_URL = setting('WPS_OUTPUT_URL', 'http://0.0.0.0:8000/wps/output/{file_name}')

DAP = setting('WPS_DAP', True)
DAP_URL = setting('WPS_DAP_URL', 'http://0.0.0.0:8000/thredds/dodsC/test/public/{file_name}')

CA_PATH = setting('WPS_CA_PATH', '/tmp/certs')

ADMIN_EMAIL = setting('WPS_ADMIN_EMAIL', 'admin@aims2.llnl.gov')

LOGIN_URL = setting('WPS_LOGIN_URL', 'http://0.0.0.0:8000/wps/home/login')
PROFILE_URL = setting('WPS_PROFILE_URL', 'http://0.0.0.0:8000/wps/home/profile')

OAUTH2_CALLBACK = setting('WPS_OAUTH2_CALLBACK', 'http://0.0.0.0:8000/auth/callback')

OPENID_TRUST_ROOT = setting('WPS_OPENID_TRUST_ROOT', 'http://0.0.0.0:8000/wps/home/login/openid')
OPENID_RETURN_TO = setting('WPS_OPENID_RETURN_TO', 'http://0.0.0.0:8000/auth/callback/openid')
OPENID_CALLBACK_SUCCESS = setting('WPS_OPENID_CALLBACK_SUCCESS', 'http://0.0.0.0:8000/wps/home/login/callback')

PASSWORD_RESET_URL = setting('WPS_PASSWORD_RESET_URL', 'http://0.0.0.0:8000/wps/home/login/reset')

FORGOT_USERNAME_SUBJECT = setting('WPS_FORGOT_USERNAME_SUBJECT', 'CWT WPS Username Recovery')
FORGOT_USERNAME_MESSAGE = setting('WPS_FORGOT_USERNAME_MESSAGE', """
Hello {username},

You've requested the recovery of your username: {username}.

Thank you,
ESGF CWT Team
                                  """)

FORGOT_PASSWORD_SUBJECT = setting('WPS_FORGOT_PASSWORD_SUBJECT', 'CWT WPS Password Reset')
FORGOT_PASSWORD_MESSAGE = setting('WPS_FORGOT_PASSWORD_MESSAGE', """
Hello {username},
<br><br>
You've request the reset of you password. Please follow this <a href="{reset_url}">link</a> to reset you password.
<br><br>
Thank you,
ESGF CWT Team
                                  """)

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
