from django.conf import settings

import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WPS_CONFIG = getattr(settings, 'WPS_CONFIG', os.path.join(BASE_DIR, 'wps.cfg'))
PROCESS_DIR = getattr(settings, 'PROCESS_DIR', os.path.join(BASE_DIR, 'processes'))

DAP_PATH_FORMAT = getattr(settings, 'DAP_PATH_FORMAT', 'http://{hostname}{port}/thredds/dodsC/test/{filename}')
DAP_HOSTNAME = getattr(settings, 'DAP_HOSTNAME', '0.0.0.0')
DAP_PORT = getattr(settings, 'DAP_PORT', 8080)

OPH_USER = getattr(settings, 'OPH_USER', 'oph-test')
OPH_PASSWORD = getattr(settings, 'OPH_PASSWORD', 'llnlaims')
OPH_HOST = getattr(settings, 'OPH_HOST', 'aims2.llnl.gov')
OPH_PORT = getattr(settings, 'OPH_PORT', 11732)
OPH_DAP_PATH_FORMAT = getattr(settings, 'OPH_DAP_PATH_FORMAT',
                              'http://{hostname}:{port}/thredds/dodsC/test/{filename}.html')
OPH_DAP_PATH = getattr(settings, 'OPH_DAP_PATH', '/export/boutte3/thredds/data/wps')
OPH_DAP_PORT = getattr(settings, 'OPH_DAP_PORT', 9001)
