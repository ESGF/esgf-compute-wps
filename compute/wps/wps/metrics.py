import os
import urllib.parse

if 'CWT_METRICS' in os.environ:
    os.environ['prometheus_multiproc_dir'] = os.environ['CWT_METRICS']

from prometheus_client import Counter # noqa
from prometheus_client import Histogram # noqa
from prometheus_client import Summary # noqa
from prometheus_client import CollectorRegistry # noqa
from prometheus_client import REGISTRY # noqa

from celery.task.control import inspect # noqa


def track_file(variable):
    parts = urllib.parse.urlparse(variable.uri)

    WPS_FILE_ACCESSED.labels(parts.hostname, parts.path,
                             variable.var_name).inc()


def track_login(counter, url):
    parts = urllib.parse.urlparse(url)

    counter.labels(parts.hostname).inc()


def jobs_queued():
    i = inspect()

    try:
        scheduled = sum(len(x) for x in list(i.scheduled().values()))
    except AttributeError:
        scheduled = 0

    try:
        reserved = sum(len(x) for x in list(i.reserved().values()))
    except AttributeError:
        reserved = 0

    return scheduled + reserved


def jobs_running():
    i = inspect()

    try:
        active = sum(len(x) for x in list(i.active().values()))
    except AttributeError:
        active = 0

    return active


def serve_metrics():
    from prometheus_client import make_wsgi_app
    from prometheus_client import multiprocess
    from wsgiref.simple_server import make_server

    multiprocess.MultiProcessCollector(WPS)

    app = make_wsgi_app(WPS)

    httpd = make_server('0.0.0.0', 8080, app)

    httpd.serve_forever()


if 'CWT_METRICS' in os.environ:
    WPS = CollectorRegistry()
else:
    WPS = REGISTRY

WPS_REGRID = Counter('wps_regrid_total', 'Number of times specific regridding'
                     ' is requested', ['tool', 'method', 'grid'])

WPS_DOMAIN_CRS = Counter('wps_domain_crs_total', 'Number of times a specific'
                         ' CRS is used', ['crs'])

WPS_DATA_DOWNLOAD = Summary('wps_data_download_seconds', 'Number of seconds'
                            ' spent downloading remote data', ['host'])
WPS_DATA_DOWNLOAD_BYTES = Counter('wps_data_download_bytes', 'Number of bytes'
                                  ' read remotely', ['host', 'variable'])

WPS_DATA_ACCESS_FAILED = Counter('wps_data_access_failed_total', 'Number'
                                 ' of times remote sites are inaccesible', ['host'])

WPS_DATA_OUTPUT = Counter('wps_data_output_bytes', 'Number of bytes written')

WPS_PROCESS_TIME = Summary('wps_process', 'Processing duration (seconds)',
                           ['identifier'])

WPS_CERT_DOWNLOAD = Counter('wps_cert_download', 'Number of times certificates'
                            ' have been downloaded')

WPS_FILE_ACCESSED = Counter('wps_file_accessed', 'Files accessed by WPS'
                            ' service', ['host', 'path', 'variable'])

WPS_JOBS_ACCEPTED = Counter('wps_jobs_accepted', 'WPS jobs accepted')
WPS_JOBS_STARTED = Counter('wps_jobs_started', 'WPS jobs started')
WPS_JOBS_SUCCEEDED = Counter('wps_jobs_succeeded', 'WPS jobs succeeded')
WPS_JOBS_FAILED = Counter('wps_jobs_failed', 'WPS jobs failed')

WPS_OPENID_LOGIN = Counter('wps_openid_login', 'ESGF OpenID login attempts', ['idp'])
WPS_OPENID_LOGIN_SUCCESS = Counter('wps_openid_login_success', 'ESGF OpenID'
                                   ' logins', ['idp'])
WPS_OAUTH_LOGIN = Counter('wps_oauth_login', 'ESGF OAuth login attempts', ['idp'])
WPS_OAUTH_LOGIN_SUCCESS = Counter('wps_oauth_login_success', 'ESGF OAuth'
                                  ' logins', ['idp'])
WPS_MPC_LOGIN = Counter('wps_mpc_login', 'ESGF MyProxyClient logins attempts', ['idp'])
WPS_MPC_LOGIN_SUCCESS = Counter('wps_mpc_login_success', 'ESGF MyProxyClient'
                                ' logins', ['idp'])

WPS_ESGF_SEARCH = Summary('wps_esgf_search', 'ESGF search duration (seconds)')
WPS_ESGF_SEARCH_SUCCESS = Counter('wps_esgf_search_success', 'Successful ESGF'
                                  ' searches')
WPS_ESGF_SEARCH_FAILED = Counter('wps_esgf_search_failed', 'Failed ESGF'
                                 ' searches')

WPS_REQUESTS = Histogram('wps_request_seconds', 'WPS request duration (seconds)',
                         ['request', 'method'])

WPS_ERRORS = Counter('wps_errors', 'WPS errors')

if __name__ == '__main__':
    from multiprocessing import Process

    server = Process(target=serve_metrics)

    server.start()

    server.join()
