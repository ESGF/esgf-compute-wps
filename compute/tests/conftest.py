import os

import pytest

def pytest_addoption(parser):
    parser.addoption('--wps-token', help='Token used for WPS authentication')
    parser.addoption('--wps-url', default='https://aims2.llnl.gov/wps', help='URL for WPS server')
    parser.addoption('--wps-noverify', action='store_false', help='Verify WPS server SSL')
    parser.addoption('--wps-timeout', default=120, type=float, help='Seconds allowed before timing out WPS request')
    parser.addoption('--wps-kernel', default='base', help='Conda kernel to run the notebooks in, must have ipykernel package installed')

@pytest.fixture
def wps_env(request):
    os.environ['WPS_URL'] = request.config.getoption('--wps-url')

    token = request.config.getoption('--wps-token')

    if token is not None:
        os.environ['WPS_TOKEN'] = request.config.getoption('--wps-token')

    os.environ['WPS_VERIFY'] = str(request.config.getoption('--wps-noverify'))

    os.environ['WPS_TIMEOUT'] = str(request.config.getoption('--wps-timeout'))

    os.environ['WPS_KERNEL'] = request.config.getoption('--wps-kernel')
