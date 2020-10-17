import os

from django.apps import AppConfig


class WpsConfig(AppConfig):
    name = 'compute_wps'

    def ready(self):
        from compute_wps import metrics # noqa
        from compute_wps import signals # noqa
        from compute_wps.exceptions import WPSError # noqa

        os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'

        from compute_wps.auth import keycloak
        from django.conf import settings
        if settings.AUTH_KEYCLOAK:
            keycloak.init(settings)
