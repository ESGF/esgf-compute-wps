import json

import requests
from celery.utils.log import get_task_logger
from django.conf import settings
from django.utils import timezone

from wps import models
from wps import WPSError
from wps.tasks import base

logger = get_task_logger('wps.tasks.metrics')


class PrometheusError(WPSError):
    pass


def query_prometheus(**kwargs):
    try:
        response = requests.get(settings.METRICS_HOST, params=kwargs,
                                timeout=(1, 30))
    except requests.ConnectionError:
        logger.exception('Error connecting to prometheus server at %r',
                         settings.METRICS_HOST)

        raise PrometheusError('Error connecting to metrics server')

    if not response.ok:
        raise WPSError('Failed querying "{}" {}: {}', settings.METRICS_HOST,
                       response.reason, response.status_code)

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


def query_health():
    user_jobs_queued = models.Job.objects.filter(status__status=models.ProcessAccepted).exclude(
        status__status=models.ProcessStarted).exclude(status__status=models.ProcessFailed).exclude(
            status__status=models.ProcessSucceeded).count()

    user_jobs_running = models.Job.objects.filter(status__status=models.ProcessStarted).exclude(
        status__status=models.ProcessFailed).exclude(status__status=models.ProcessSucceeded).count()

    data = {
        'user_jobs_running': user_jobs_running,
        'user_jobs_queued': user_jobs_queued,
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


def query_usage():
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

    file_count = query_multiple_value('url', query=FILE_CNT)

    file = {}

    try:
        for item in list(file_count.keys()):
            logger.info('%r', item)

            try:
                url_obj = models.File.objects.filter(url=item)[0]
            except IndexError:
                count = 0
            else:
                count = url_obj.userfile_set.all().distinct('user').count()

            file[item] = {'count': file_count[item], 'unique_users': count}
    except AttributeError:
        file['files'] = 'Unavailable'

    data = {
        'files': file,
        'operators': operator,
    }

    return data


@base.register_process('CDAT', 'metrics', abstract=METRICS_ABSTRACT)
@base.cwt_shared_task()
def metrics_task(self, context):
    data = {
        'time': timezone.now().ctime(),
    }

    data.update(health=query_health())

    data.update(usage=query_usage())

    context.output.append(json.dumps(data))

    return context
