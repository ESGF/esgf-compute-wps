import os

os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'

default_app_config = 'wps.apps.WpsConfig'

class WPSError(Exception):
    def __init__(self, message, *args, **kwargs):
        super(WPSError, self).__init__(message.format(*args, **kwargs))

class AccessError(WPSError):
    def __init__(self, url, error):
        msg = 'Error accessing {}: {}'

        super(AccessError, self).__init__(msg, url, error)

