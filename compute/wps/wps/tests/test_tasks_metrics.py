import mock
import requests
from django import test
from django.conf import settings

from wps.tasks import metrics_
from wps import WPSError


class TaskMetricsTestCase(test.TestCase):

    @mock.patch.object(requests, 'get')
    @mock.patch('wps.models.Job.objects.filter')
    def test_health(self, mock_filter, mock_get):
        mock_get.return_value.json.return_value = {
            'status': 'ok',
            'data': {
                'result': [
                    {
                        'metric': 'test',
                        'value': [],
                    },
                ]
            }
        }

        mock_filter.return_value.exclude.return_value.exclude.return_value.count.return_value = 2

        mock_filter.return_value.exclude.return_value.exclude.return_value.exclude.return_value.count.return_value = 2

        context = mock.MagicMock()

        metrics_.metrics_task(context)

        self.assertEqual(mock_get.call_count, 10)

    @mock.patch.object(requests, 'get')
    def test_query_multiple_value_custom_type(self, mock_get):
        type(mock_get.return_value).ok = mock.PropertyMock(return_value=True)

        mock_get.return_value.json.return_value = {
            'status': 'ok',
            'data': {
                'result': [
                    {'metric': {'request': 'GetCapabilities'}, 'value': [32.2,
                                                                         '10.2']},
                    {'metric': {'request': 'DescribeProcess'}, 'value': [32.2,
                                                                         '32.2']},
                ]
            }
        }

        result = metrics_.query_multiple_value('request', type=float, query='sum(wps_requests)')

        self.assertIsInstance(result, dict)
        self.assertEqual(result, {'GetCapabilities': 10.2, 'DescribeProcess':
                                  32.2})

    @mock.patch.object(requests, 'get')
    def test_query_multiple_value(self, mock_get):
        type(mock_get.return_value).ok = mock.PropertyMock(return_value=True)

        mock_get.return_value.json.return_value = {
            'status': 'ok',
            'data': {
                'result': [
                    {'metric': {'request': 'GetCapabilities'}, 'value': [32.2, 10]},
                    {'metric': {'request': 'DescribeProcess'}, 'value': [32.2, 32]},
                ]
            }
        }

        result = metrics_.query_multiple_value('request', query='sum(wps_requests)')

        self.assertIsInstance(result, dict)
        self.assertEqual(result, {'GetCapabilities': 10, 'DescribeProcess':
                                  32})

    @mock.patch.object(requests, 'get')
    def test_query_single_value_custom_type(self, mock_get):
        type(mock_get.return_value).ok = mock.PropertyMock(return_value=True)

        mock_get.return_value.json.return_value = {
            'status': 'ok',
            'data': {
                'result': [
                    {'value': [32.2, 2.1]},
                ]
            }
        }

        result = metrics_.query_single_value(type=float, query='sum(wps_requests)')

        self.assertIsInstance(result, float)
        self.assertEqual(result, 2.1)

    @mock.patch.object(requests, 'get')
    def test_query_single_value_empty_result(self, mock_get):
        type(mock_get.return_value).ok = mock.PropertyMock(return_value=True)

        mock_get.return_value.json.return_value = {
            'status': 'ok',
            'data': {
                'result': []
            }
        }

        result = metrics_.query_single_value(query='sum(wps_requests)')

        self.assertIsInstance(result, int)
        self.assertEqual(result, 0)

    @mock.patch.object(requests, 'get')
    def test_query_single_value(self, mock_get):
        type(mock_get.return_value).ok = mock.PropertyMock(return_value=True)

        mock_get.return_value.json.return_value = {
            'status': 'ok',
            'data': {
                'result': [
                    {'value': [32.2, 2]},
                ]
            }
        }

        result = metrics_.query_single_value(query='sum(wps_requests)')

        self.assertIsInstance(result, int)
        self.assertEqual(result, 2)

    @mock.patch.object(requests, 'get')
    def test_query_prometheus_malformed(self, mock_get):
        type(mock_get.return_value).ok = mock.PropertyMock(return_value=True)

        mock_get.return_value.json.return_value = {
            'data': {
                'result': {
                    'data': 3.2,
                }
            }
        }

        with self.assertRaises(WPSError):
            metrics_.query_prometheus(query='sum(wps_requests)')

    @mock.patch.object(requests, 'get')
    def test_query_prometheus_not_ok(self, mock_get):
        type(mock_get.return_value).ok = mock.PropertyMock(return_value=False)

        mock_get.return_value.json.return_value = {
            'status': 'ok',
            'data': {
                'result': {
                    'data': 3.2,
                }
            }
        }

        with self.assertRaises(WPSError):
            metrics_.query_prometheus(query='sum(wps_requests)')

    @mock.patch.object(requests, 'get')
    def test_query_prometheus(self, mock_get):
        type(mock_get.return_value).ok = mock.PropertyMock(return_value=True)

        mock_get.return_value.json.return_value = {
            'status': 'ok',
            'data': {
                'result': {
                    'data': 3.2,
                }
            }
        }

        result = metrics_.query_prometheus(query='sum(wps_requests)')

        self.assertEqual(result, {'data': 3.2})

        mock_get.assert_called_with(settings.METRICS_HOST, params={'query':
                                                                   'sum(wps_requests)'}, timeout=(1, 30))
