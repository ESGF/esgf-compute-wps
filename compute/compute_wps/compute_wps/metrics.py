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

WPS_CERT_DOWNLOAD = Counter('wps_cert_download', 'Number of times certificates  have been downloaded')

WPS_JOB_STATE = Counter('wps_job_state', 'WPS job state count', ['state'])

WPS_OPENID_LOGIN = Counter('wps_openid_login', 'ESGF OpenID login attempts', ['idp'])
WPS_OPENID_LOGIN_SUCCESS = Counter('wps_openid_login_success', 'ESGF OpenID logins', ['idp'])
WPS_OAUTH_LOGIN = Counter('wps_oauth_login', 'ESGF OAuth login attempts', ['idp'])
WPS_OAUTH_LOGIN_SUCCESS = Counter('wps_oauth_login_success', 'ESGF OAuth logins', ['idp'])
WPS_MPC_LOGIN = Counter('wps_mpc_login', 'ESGF MyProxyClient logins attempts', ['idp'])
WPS_MPC_LOGIN_SUCCESS = Counter('wps_mpc_login_success', 'ESGF MyProxyClient logins', ['idp'])

WPS_REQUESTS = Histogram('wps_request_seconds', 'WPS request duration (seconds)', ['request', 'method'])

WPS_ERRORS = Counter('wps_errors', 'WPS errors')
