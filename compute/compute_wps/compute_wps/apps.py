import os
import logging

from django.apps import AppConfig

logger = logging.getLogger('compute_wps.app')

class WpsConfig(AppConfig):
    name = 'compute_wps'

    def ready(self):
        from compute_wps import metrics # noqa
        from compute_wps.exceptions import WPSError # noqa

        from compute_wps.auth import keycloak
        from django.conf import settings

        logger.info(f'Loading settings from {settings.DJANGO_CONFIG_PATH}')

        if settings.AUTH_KEYCLOAK:
            keycloak.init(settings)
