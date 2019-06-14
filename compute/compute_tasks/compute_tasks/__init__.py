import os

os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'


class WPSError(Exception):
    def __init__(self, fmt=None, *args, **kwargs):
        if fmt is None:
            fmt = ''

        self.msg = fmt.format(*args, **kwargs)

    def __str__(self):
        return self.msg


class AccessError(WPSError):
    def __init__(self, url, error):
        super(AccessError, self).__init__('Access error {!r}: {!s}', url, error)


class DaskClusterAccessError(WPSError):
    def __init__(self):
        super(DaskClusterAccessError, self).__init__('Error connecting to dask scheduler')
