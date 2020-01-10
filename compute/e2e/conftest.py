import os

import cwt
import pytest

os.environ['CDAT_ANONYMOUS_LOG'] = 'yes'


def int_or_float(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def convert_value(value):
    if value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    else:
        return int_or_float(value)


@pytest.fixture(scope='session')
def wps_client(request):
    wps_url = request.config.getoption('--wps-url')

    compute_token = request.config.getoption('--compute-token')

    extra = dict((x[0], convert_value(x[1])) for x in request.config.getoption('--extra'))

    client = cwt.WPSClient(wps_url, compute_token=compute_token, **extra)

    return client


def pytest_addoption(parser):
    parser.addoption('--wps-url', action='store')

    parser.addoption('--compute-token', action='store')

    parser.addoption('--extra', action='append', nargs=2, default=[])
