import os
import sys

if 'CWT_METRICS' in os.environ:
    os.environ['prometheus_multiproc_dir'] = os.environ['CWT_METRICS']

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from prometheus_client import Summary
from prometheus_client import CollectorRegistry

WPS = CollectorRegistry()

JOBS_QUEUED = Gauge('wps_jobs_queued', 'Number of jobs queued')

JOBS_RUNNING = Gauge('wps_jobs_running', 'Number of jobs running')

UPLOAD_BYTES = Counter('wps_upload_bytes', 'Number of uploaded bytes')

INGRESS_BYTES = Counter('wps_ingress_bytes', 'Number of ingressed bytes', ['host'])

INGRESS_SECONDS = Counter('wps_ingress_seconds', 'Number of seconds spent'
                          'ingressing data', ['host'])

CACHE_BYTES = Gauge('wps_cache_bytes', 'Number of cached bytes')

CACHE_FILES = Gauge('wps_cache_files', 'Number of cached files')

WPS_CAPABILITIES = Summary('wps_get_capabilities_seconds',
                             'WPS GetCapabilities', ['method'])

WPS_CAPABILITIES_GET = WPS_CAPABILITIES.labels('get')
WPS_CAPABILITIES_POST = WPS_CAPABILITIES.labels('post')

WPS_DESCRIBE = Summary('wps_describe_process_seconds', 'WPS DescribeProcess', 
                         ['method'])

WPS_DESCRIBE_GET = WPS_DESCRIBE.labels('get')
WPS_DESCRIBE_POST = WPS_DESCRIBE.labels('post')

WPS_EXECUTE = Summary('wps_execute_seconds', 'WPS Execute', ['method'])

WPS_EXECUTE_GET = WPS_EXECUTE.labels('get')
WPS_EXECUTE_POST = WPS_EXECUTE.labels('post')

WPS_ERRORS = Counter('wps_errors', 'WPS Errors')

def serve_metrics():
    from prometheus_client import make_wsgi_app
    from prometheus_client import multiprocess
    from wsgiref.simple_server import make_server

    multiprocess.MultiProcessCollector(WPS)

    app = make_wsgi_app(WPS)

    httpd = make_server('0.0.0.0', 8080, app)

    httpd.serve_forever()

if __name__ == '__main__':
    from multiprocessing import Process

    server = Process(target=serve_metrics)

    server.start()

    server.join()
