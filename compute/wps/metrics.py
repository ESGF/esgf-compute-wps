import os

# Tell prometheus to operate in multiprocess mode. See compute/celery.py for
# details.
if 'CWT_METRICS' in os.environ:
    os.environ['prometheus_multiproc_dir'] = '/tmp/cwt_metrics'

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from prometheus_client import Summary
from prometheus_client import CollectorRegistry

CELERY = CollectorRegistry(auto_describe=True)

INGRESS_BYTES = Counter('wps_ingress_bytes', 'Number of ingressed bytes',
                        ['host'], registry=CELERY)

CACHE_BYTES = Summary('wps_cache_bytes', 'Number of cached bytes',
                    registry=CELERY)

CACHE_FILES = Summary('wps_cache_files', 'Number of cached files',
                    registry=CELERY)

WPS = CollectorRegistry(auto_describe=True)

WPS_CAPABILITIES = Summary('wps_get_capabilities_seconds',
                             'WPS GetCapabilities', ['method'],
                             registry=WPS)

WPS_CAPABILITIES_GET = WPS_CAPABILITIES.labels('get')
WPS_CAPABILITIES_POST = WPS_CAPABILITIES.labels('post')

WPS_DESCRIBE = Summary('wps_describe_process_seconds', 'WPS DescribeProcess', 
                         ['method'], registry=WPS)

WPS_DESCRIBE_GET = WPS_DESCRIBE.labels('get')
WPS_DESCRIBE_POST = WPS_DESCRIBE.labels('post')

WPS_EXECUTE = Summary('wps_execute_seconds', 'WPS Execute', ['method'],
                        registry=WPS)

WPS_EXECUTE_GET = WPS_EXECUTE.labels('get')
WPS_EXECUTE_POST = WPS_EXECUTE.labels('post')

WPS_ERRORS = Counter('wps_errors', 'WPS Errors', registry=WPS)
