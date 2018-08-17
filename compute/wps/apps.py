from __future__ import unicode_literals

import os

from django.apps import AppConfig

class WpsConfig(AppConfig):
    name = 'wps'

    def ready(self):
        from wps import metrics
        from wps import settings
        from wps import signals

        os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'
