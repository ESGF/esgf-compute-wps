from __future__ import unicode_literals

import os

from django.apps import AppConfig

from compute_settings import settings


class WpsConfig(AppConfig):
    name = 'compute_wps'

    def ready(self):
        from django.conf import settings as wps_settings
        from compute_wps import metrics # noqa
        from compute_wps import WPSError # noqa
        from compute_wps import signals # noqa

        os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'

        settings.patch_settings(wps_settings)
