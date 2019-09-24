def pytest_addoption(parser):
    parser.addoption('--wps-url', action='store')

    parser.addoption('--compute-token', action='store')

    parser.addoption('--extra', action='append', nargs=2)

    parser.addoption('--site', action='store', default='llnl')

    parser.addoption('--variable', action='store', default='tas')
