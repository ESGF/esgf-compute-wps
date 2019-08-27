import pytest
import requests

from compute_tasks import metrics_ as metrics
from compute_tasks import WPSError


def test_query_prometheus_missing_status(mocker):
    mocker.patch.object(metrics, 'requests')

    metrics.requests.get.return_value.json.return_value = {
        'data': {
            'result': 10,
        },
    }

    with pytest.raises(WPSError):
        metrics.query_prometheus(verify=False)


def test_query_prometheus_not_ok(mocker):
    mocker.patch.object(metrics, 'requests')

    metrics.requests.get.return_value.ok = False

    with pytest.raises(WPSError):
        metrics.query_prometheus(verify=False)


def test_query_prometheus_connection_error(mocker):
    mocker.patch.object(metrics, 'requests')

    metrics.requests.get.side_effect = requests.exceptions.ConnectionError

    with pytest.raises(metrics.PrometheusError):
        metrics.query_prometheus(verify=False)


def test_query_prometheus(mocker):
    mocker.patch.object(metrics, 'requests')

    metrics.requests.get.return_value.json.return_value = {
        'status': 'up',
        'data': {
            'result': 10,
        },
    }

    output = metrics.query_prometheus(verify=False)

    metrics.requests.get.assert_called_with('/prometheus/api/v1/query', params={'verify': False}, timeout=(1, 30))

    assert output == 10


def test_query_single_value_missing_key(mocker):
    mocker.patch.object(metrics, 'query_prometheus')

    metrics.query_prometheus.return_value = []

    output = metrics.query_single_value(verify=False)

    metrics.query_prometheus.assert_called_with(verify=False)

    assert output == 0


def test_query_single_value_missing_value(mocker):
    mocker.patch.object(metrics, 'query_prometheus')

    metrics.query_prometheus.return_value = [
        {
            'metric': {
                'test': 'test_metric',
            },
        },
    ]

    output = metrics.query_single_value(verify=False)

    metrics.query_prometheus.assert_called_with(verify=False)

    assert output == 0


def test_query_single_value(mocker):
    mocker.patch.object(metrics, 'query_prometheus')

    metrics.query_prometheus.return_value = [
        {
            'metric': {
                'test': 'test_metric',
            },
            'value': [0, '2'],
        },
    ]

    output = metrics.query_single_value(verify=False)

    metrics.query_prometheus.assert_called_with(verify=False)

    assert output == 2


def test_query_multiple_value_missing_key(mocker):
    mocker.patch.object(metrics, 'query_prometheus')

    metrics.query_prometheus.return_value = [
        {
            'metric': {},
            'value': [0, '2'],
        },
    ]

    metrics.query_multiple_value('test', verify=False)

    metrics.query_prometheus.assert_called_with(verify=False)


def test_query_multiple_value_missing_value(mocker):
    mocker.patch.object(metrics, 'query_prometheus')

    metrics.query_prometheus.return_value = [
        {
            'metric': {
                'test': 'test_metric',
            },
        },
    ]

    output = metrics.query_multiple_value('test', verify=False)

    metrics.query_prometheus.assert_called_with(verify=False)

    assert output['test_metric'] == 0


def test_query_multiple_value(mocker):
    mocker.patch.object(metrics, 'query_prometheus')

    metrics.query_prometheus.return_value = [
        {
            'metric': {
                'test': 'test_metric',
            },
            'value': [0, '2'],
        },
    ]

    output = metrics.query_multiple_value('test', verify=False)

    metrics.query_prometheus.assert_called_with(verify=False)

    assert output['test_metric'] == 2


def test_quyery_health(mocker):
    mocker.patch.object(metrics, 'query_single_value')

    context = mocker.MagicMock()
    context.unique_status.return_value = {
        'ProcessAccepted': 10,
        'ProcessStarted': 20,
    }

    output = metrics.query_health(context)

    assert output['user_jobs_running'] == 10
    assert output['user_jobs_queued'] == 20

    metrics.query_single_value.assert_any_call(type=float, query=metrics.CPU_AVG_5m)
    metrics.query_single_value.assert_any_call(type=float, query=metrics.CPU_AVG_1h)
    metrics.query_single_value.assert_any_call(type=int, query=metrics.CPU_CNT)
    metrics.query_single_value.assert_any_call(type=float, query=metrics.MEM_AVG_5m)
    metrics.query_single_value.assert_any_call(type=float, query=metrics.MEM_AVG_1h)
    metrics.query_single_value.assert_any_call(type=int, query=metrics.MEM_AVAIL)
    metrics.query_single_value.assert_any_call(type=int, query=metrics.WPS_REQ)
    metrics.query_single_value.assert_any_call(type=float, query=metrics.WPS_REQ_AVG_5m)


def test_query_usage_missing(mocker):
    mocker.patch.object(metrics, 'query_multiple_value')

    metrics.query_multiple_value.side_effect = [
        None,
        {
            'test': 1.05,
        }
    ]

    context = mocker.MagicMock()

    context.files_distinct_users.return_value = {}

    data = metrics.query_usage(context)

    metrics.query_multiple_value.assert_any_call('request', type=float, query=metrics.WPS_REQ_SUM)
    metrics.query_multiple_value.assert_any_call('request', type=float, query=metrics.WPS_REQ_AVG)

    assert data['files'] == {}
    assert data['operators']['operations'] == 'Unavailable'


def test_query_usage(mocker):
    mocker.patch.object(metrics, 'query_multiple_value')

    metrics.query_multiple_value.side_effect = [
        {
            'test': 10,
        },
        {
            'test': 1.05,
        }
    ]

    context = mocker.MagicMock()

    context.files_distinct_users.return_value = {}

    data = metrics.query_usage(context)

    metrics.query_multiple_value.assert_any_call('request', type=float, query=metrics.WPS_REQ_SUM)
    metrics.query_multiple_value.assert_any_call('request', type=float, query=metrics.WPS_REQ_AVG)

    assert data['files'] == {}
    assert data['operators'] == {'test': {'count': 10, 'avg_time': 1.05}}


def test_metrics_task(mocker):
    datetime = mocker.patch.object(metrics, 'datetime')

    datetime.now.return_value.ctime.return_value = 0

    mocker.patch.object(metrics, 'query_health')
    mocker.patch.object(metrics, 'query_usage')

    metrics.query_health.return_value = ''
    metrics.query_usage.return_value = ''

    context = mocker.MagicMock()

    metrics.metrics_task(context)

    datetime.now.assert_called()
    datetime.now.return_value.ctime.assert_called()

    metrics.query_health.assert_called_with(context)
    metrics.query_usage.assert_called_with(context)

    context.output.append.assert_called()


def test_serve_metrics(mocker):
    cr = mocker.patch('prometheus_client.CollectorRegistry')
    wsgi = mocker.patch('prometheus_client.make_wsgi_app')
    mp = mocker.patch('prometheus_client.multiprocess.MultiProcessCollector')
    ms = mocker.patch('wsgiref.simple_server.make_server')

    metrics.serve_metrics()

    mp.assert_called_with(cr.return_value)

    wsgi.assert_called_with(cr.return_value)

    ms.assert_called_with('0.0.0.0', 8080, wsgi.return_value)

    ms.return_value.serve_forever.assert_called()


def test_metrics_main(mocker):
    proc = mocker.patch('multiprocessing.Process')

    metrics.main()

    proc.assert_called_with(target=metrics.serve_metrics)

    proc.return_value.start.assert_called()

    proc.return_value.join.assert_called()
