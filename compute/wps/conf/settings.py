from django.conf import settings

import os

def get_setting(name, default):
    return getattr(settings, name, default)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

WPS_CONFIG = get_setting('WPS_CONFIG', os.path.join(BASE_DIR, 'wps.cfg'))

PROCESS_DIR = get_setting('PROCESS_DIR', os.path.join(BASE_DIR, 'processes'))

DAP_HOSTNAME = get_setting('DAP_HOSTNAME', '0.0.0.0')

DAP_PORT = get_setting('DAP_PORT', 8001)
