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

WPS_CERT_DOWNLOAD = Counter('wps_cert_download', 'Number of times certificates  have been downloaded', registry=WPS)

WPS_JOB_STATE = Counter('wps_job_state', 'WPS job state count', ['state'], registry=WPS)

WPS_REQUESTS = Histogram('wps_request_seconds', 'WPS request duration (seconds)', ['request', 'method'], registry=WPS)

WPS_ERRORS = Counter('wps_errors', 'WPS errors', registry=WPS)
