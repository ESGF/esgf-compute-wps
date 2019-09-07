def pytest_addoption(parser):
    parser.addoption('--wps-url', action='store')

    parser.addoption('--compute-token', action='store')

    parser.addoption('--extra', action='append', nargs=2)
