import os
import sys

if 'CWT_METRICS' in os.environ:
    os.environ['prometheus_multiproc_dir'] = os.environ['CWT_METRICS']

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from prometheus_client import Summary
from prometheus_client import CollectorRegistry
from prometheus_client import REGISTRY

from celery.task.control import inspect

def jobs_queued():
    i = inspect()

    try:
        scheduled = sum(len(x) for x in i.scheduled().values())
    except AttributeError:
        scheduled = 0

    try:
        reserved = sum(len(x) for x in i.reserved().values())
    except AttributeError:
        reserved = 0

    return scheduled + reserved

def jobs_running():
    i = inspect()

    try:
        active = sum(len(x) for x in i.active().values())
    except AttributeError:
        active = 0

    return active

if 'CWT_METRICS' in os.environ:
    WPS = CollectorRegistry()
else:
    WPS = REGISTRY

USERS_ACTIVE = Gauge('wps_users_active', 'Number of active users')

USERS = Counter('wps_users', 'Number of registered users')

JOBS_QUEUED = Gauge('wps_jobs_queued', 'Number of jobs queued',
                    multiprocess_mode='livesum')

JOBS_RUNNING = Gauge('wps_jobs_running', 'Number of jobs running',
                     multiprocess_mode='livesum')

PROCESS_BYTES = Counter('wps_process_bytes', 'Number of bytes processed',
                        ['identifier'])

PROCESS_SECONDS = Counter('wps_process_seconds', 'NUmber of seconds spent'
                          'processing data', ['identifier'])

INGRESS_BYTES = Counter('wps_ingress_bytes', 'Number of ingressed bytes', ['host'])

INGRESS_SECONDS = Counter('wps_ingress_seconds', 'Number of seconds spent'
                          'ingressing data', ['host'])

CACHE_BYTES = Gauge('wps_cache_bytes', 'Number of cached bytes',
                    multiprocess_mode='livesum')

CACHE_FILES = Gauge('wps_cache_files', 'Number of cached files',
                    multiprocess_mode='livesum')

WPS_OPERATION = Counter('wps_operation', 'Number of times an operation has been'
                        'requested', ['identifier'])

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
