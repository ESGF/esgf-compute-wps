from __future__ import unicode_literals

import os

from django.apps import AppConfig

from wps import settings as wps_settings

class WpsConfig(AppConfig):
    name = 'wps'

    def ready(self):
        from django.conf import settings
        from wps import metrics
        from wps import signals
        from wps import WPSError

        os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'

        wps_settings.patch_settings(settings)
