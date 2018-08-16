import glob
import os
import sys

# Tell prometheus to operate in multiprocess mode. See compute/celery.py for
# details.
if 'CWT_METRICS' in os.environ:
    os.environ['prometheus_multiproc_dir'] = '/tmp/cwt_metrics'

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from prometheus_client import Summary
from prometheus_client import CollectorRegistry
from prometheus_client import make_wsgi_app
from prometheus_client import multiprocess
from wsgiref.simple_server import make_server

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

def run():
    try:
        metrics_path = os.environ['CWT_METRICS']
    except KeyError:
        print 'Must set environment variable CWT_METRICS to the shared path to store metrics'

        os.exit(1)

    # Need to clear out any residual files
    files = glob.glob('{}/*'.format(metrics_path))

    for f in files:
        os.remove(f)

    # Tell prometheus_client to operate in multiprocess mode. Documentation is
    # here https://github.com/prometheus/client_python, though it's not clear.
    # Basically in multiprocess mode metrics are written to a shared directory
    # where they are aggregate and served up.
    os.environ['prometheus_multiproc_dir'] = metrics_path
    os.environ['DJANGO_SETTINGS_MODULE'] = 'compute.settings'

    sys.path.insert(0, '/var/www/compute/compute')

    from wps import metrics

    # converts the registry to a multiprocess registry, allowing for the
    # aggregation of the metrics between threads.
    multiprocess.MultiProcessCollector(metrics.CELERY)

    app = make_wsgi_app(registry=metrics.CELERY)

    httpd = make_server('0.0.0.0', 8080, app)

    httpd.serve_forever()

if __name__ == '__main__':
    run()
