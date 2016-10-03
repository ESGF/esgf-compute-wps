from django.conf import settings

import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

WPS_CONFIG = getattr(settings, 'WPS_CONFIG', os.path.join(BASE_DIR, 'wps.cfg'))

PROCESS_DIR = getattr(settings, 'PROCESS_DIR', os.path.join(BASE_DIR, 'processes'))

DAP_PATH_FORMAT = getattr(settings, 'DAP_PATH_FORMAT', 'http://{hostname}{port}/thredds/dodsC/test/{filename}')

DAP_HOSTNAME = getattr(settings, 'DAP_HOSTNAME', '0.0.0.0')

DAP_PORT = getattr(settings, 'DAP_PORT', 8080)
