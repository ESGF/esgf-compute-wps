import os

import mock
import cwt
from django import test

from wps import metrics

class MetricsTestCase(test.TestCase):
    @mock.patch('wsgiref.simple_server.make_server')
    @mock.patch('prometheus_client.make_wsgi_app')
    def test_serve_metrics(self, mock_app, mock_make):
        os.environ['prometheus_multiproc_dir'] = '/'

        metrics.serve_metrics()

        mock_make.assert_called_with('0.0.0.0', 8080, mock_app.return_value)

    @mock.patch('wps.metrics.inspect')
    def test_jobs_running(self, mock_inspect):
        mock_inspect.return_value.active.side_effect = AttributeError()

        result = metrics.jobs_running()

        self.assertEqual(result, 0)

    @mock.patch('wps.metrics.inspect')
    def test_jobs_running(self, mock_inspect):
        mock_inspect.return_value.active.return_value.values.return_value = [[{}]]

        result = metrics.jobs_running()

        self.assertEqual(result, 1)

    @mock.patch('wps.metrics.inspect')
    def test_jobs_queued_empty(self, mock_inspect):
        mock_inspect.return_value.scheduled.side_effect = AttributeError()

        result = metrics.jobs_queued()

        self.assertEqual(result, 0)

    @mock.patch('wps.metrics.inspect')
    def test_jobs_queued(self, mock_inspect):
        mock_inspect.return_value.scheduled.return_value.values.return_value = [[{}]]

        mock_inspect.return_value.reserved.return_value.values.return_value = [[{}]]

        result = metrics.jobs_queued()

        self.assertEqual(result, 2)
