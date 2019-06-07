import urllib.parse

from prometheus_client import Counter # noqa
from prometheus_client import Histogram # noqa
from prometheus_client import Summary # noqa
from prometheus_client import CollectorRegistry # noqa
from prometheus_client import REGISTRY # noqa


def track_login(counter, url):
    parts = urllib.parse.urlparse(url)

    counter.labels(parts.hostname).inc()


WPS = REGISTRY

WPS_DATA_ACCESS_FAILED = Counter('wps_data_access_failed_total',
                                 'Number of times remote sites are inaccesible', ['host'])

WPS_CERT_DOWNLOAD = Counter('wps_cert_download', 'Number of times certificates'
                            ' have been downloaded')

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
