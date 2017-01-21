from django.conf import settings

import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CDAS_HOST = getattr(settings, 'CDAS_HOST', 'localhost')
CDAS_REQUEST_PORT = getattr(settings, 'CDAS_REQUEST_PORT', 4360)
CDAS_RESPONSE_PORT = getattr(settings, 'CDAS_RESPONSE_PORT', 4361)
