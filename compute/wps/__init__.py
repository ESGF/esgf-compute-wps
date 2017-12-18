import os

os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'

default_app_config = 'wps.apps.WpsConfig'

class WPSError(Exception):
    def __init__(self, message, **kwargs):
        super(WPSError, self).__init__(message.format(**kwargs))
