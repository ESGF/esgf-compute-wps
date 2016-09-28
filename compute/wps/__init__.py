import os
import logging

os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'

logger = logging.getLogger(__name__)

default_app_config = 'wps.apps.WpsConfig'
