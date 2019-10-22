def pytest_addoption(parser):
    parser.addoption('--wps-url', action='store')

    parser.addoption('--compute-token', action='store')

    parser.addoption('--extra', action='append', nargs=2, default=[])

    parser.addoption('--site', action='store', default='ucar')

    parser.addoption('--variable', action='store', default='tas')
