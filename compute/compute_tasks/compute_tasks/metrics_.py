import os
import json
from datetime import datetime

import requests
from celery.utils.log import get_task_logger
from prometheus_client import Counter # noqa
from prometheus_client import Histogram # noqa
from prometheus_client import Summary # noqa

from compute_tasks import base
from compute_tasks import WPSError

logger = get_task_logger('compute_tasks.metrics')

PROMETHEUS_HOST = os.environ.get('PROMETHEUS_HOST', '')


class PrometheusError(WPSError):
    pass


WPS_REGRID = Counter('wps_regrid_total', 'Number of times specific regridding is requested', ['tool', 'method', 'grid'])

WPS_DOMAIN_CRS = Counter('wps_domain_crs_total', 'Number of times a specific CRS is used for a dimension',
                         ['dimension', 'crs'])

WPS_DATA_SRC_BYTES = Counter('wps_data_src_bytes', 'Number of bytes in the source')

WPS_DATA_IN_BYTES = Counter('wps_data_in_bytes', 'Number of bytes read from source')

WPS_DATA_OUT_BYTES = Counter('wps_data_out_bytes', 'Number of bytes written')

WPS_DATA_ACCESS_FAILED = Counter('wps_data_access_failed_total',
                                 'Number of times remote sites are inaccesible', ['host'])

WPS_PROCESS_TIME = Summary('wps_process', 'Processing duration (seconds)', ['identifier', 'state'])

WPS_FILE_ACCESSED = Counter('wps_file_accessed', 'Files accessed by WPS service', ['host', 'variable'])


def query_prometheus(**kwargs):
    url = '{!s}/prometheus/api/v1/query'.format(PROMETHEUS_HOST)

    try:
        response = requests.get(url, params=kwargs, timeout=(1, 30))
    except requests.ConnectionError:
        logger.exception('Error connecting to prometheus server at %r', PROMETHEUS_HOST)

        raise PrometheusError('Error connecting to metrics server')

    if not response.ok:
        raise WPSError('Failed querying "{}" {}: {}', PROMETHEUS_HOST, response.reason, response.status_code)

    data = response.json()

    try:
        data['status']
    except KeyError:
        raise WPSError('Excepted JSON from prometheus request')

    logger.info('%r', data)

    return data['data']['result']


def query_single_value(type=int, **kwargs):
    try:
        data = query_prometheus(**kwargs)[0]
    except IndexError:
        return type()

    try:
        return type(data['value'][1])
    except (KeyError, IndexError):
        return type()


def query_multiple_value(key, type=int, **kwargs):
    results = {}

    data = query_prometheus(**kwargs)

    for item in data:
        try:
            name = item['metric'][key]
        except (KeyError, TypeError):
            continue

        try:
            value = item['value'][1]
        except (KeyError, IndexError):
            results[name] = type()
        else:
            results[name] = type(value)

    return results


METRICS_ABSTRACT = """
Returns the current metrics of the server.
"""

CPU_AVG_5m = 'sum(rate(container_cpu_usage_seconds_total{container_name=~".*(dask|celery).*"}[5m]))'
CPU_AVG_1h = 'sum(rate(container_cpu_usage_seconds_total{container_name=~".*(dask|celery).*"}[1h]))'
CPU_CNT = 'sum(machine_cpu_cores)'
MEM_AVG_5m = 'sum(avg_over_time(container_memory_usage_bytes{container_name=~".*(dask|celery).*"}[5m]))'
MEM_AVG_1h = 'sum(avg_over_time(container_memory_usage_bytes{container_name=~".*(dask|celery).*"}[1h]))'
MEM_AVAIL = 'sum(container_memory_max_usage_bytes{container_name=~".*(dask|celery).*"})'
WPS_REQ = 'sum(wps_request_seconds_count)'
WPS_REQ_AVG_5m = 'sum(avg_over_time(wps_request_seconds_count[5m]))'


def query_health(context):
    status = context.unique_status()

    data = {
        'user_jobs_running': status.get('ProcessAccepted', 0),
        'user_jobs_queued': status.get('ProcessStarted', 0),
        'cpu_avg_5m': query_single_value(type=float, query=CPU_AVG_5m),
        'cpu_avg_1h': query_single_value(type=float, query=CPU_AVG_1h),
        'cpu_count': query_single_value(type=int, query=CPU_CNT),
        'memory_usage_avg_bytes_5m': query_single_value(type=float, query=MEM_AVG_5m),
        'memory_usage_avg_bytes_1h': query_single_value(type=float, query=MEM_AVG_1h),
        'memory_available': query_single_value(type=int, query=MEM_AVAIL),
        'wps_requests': query_single_value(type=int, query=WPS_REQ),
        'wps_requests_avg_5m': query_single_value(type=float, query=WPS_REQ_AVG_5m),
    }

    return data


WPS_REQ_SUM = 'sum(wps_request_seconds_count) by (request)'
WPS_REQ_AVG = 'avg(wps_request_seconds_sum) by (request)'
FILE_CNT = 'sum(wps_file_accessed{url!=""}) by (url)'


def query_usage(context):
    operator_count = query_multiple_value('request', type=float, query=WPS_REQ_SUM)

    operator_avg_time = query_multiple_value('request', type=float, query=WPS_REQ_AVG)

    operator = {}

    try:
        for item in set(list(operator_count.keys())+list(operator_avg_time.keys())):
            operator[item] = {}

            if item in operator_count:
                operator[item]['count'] = operator_count[item]

            if item in operator_avg_time:
                operator[item]['avg_time'] = operator_avg_time[item]
    except AttributeError:
        operator['operations'] = 'Unavailable'

    data = {
        'files': context.files_unique_users(),
        'operators': operator,
    }

    return data


@base.register_process('CDAT', 'metrics', abstract=METRICS_ABSTRACT)
@base.cwt_shared_task()
def metrics_task(self, context):
    data = {
        'time': datetime.now().ctime(),
    }

    data.update(health=query_health(context))

    data.update(usage=query_usage(context))

    context.output.append(json.dumps(data))

    return context


def serve_metrics():
    from prometheus_client import CollectorRegistry
    from prometheus_client import make_wsgi_app
    from prometheus_client import multiprocess
    from wsgiref.simple_server import make_server

    WPS = CollectorRegistry()

    multiprocess.MultiProcessCollector(WPS)

    app = make_wsgi_app(WPS)

    httpd = make_server('0.0.0.0', 8080, app)

    httpd.serve_forever()


def main():
    from multiprocessing import Process

    server = Process(target=serve_metrics)

    server.start()

    server.join()
