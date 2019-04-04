import mock
import unittest

from cwt_kubernetes.cluster_manager import ClusterManager


class TestClusterManager(unittest.TestCase):
    @mock.patch('cwt_kubernetes.cluster_manager.Client')
    def test_find_stale_workers_scheduler_exception(self, mock_client):
        mock_client.return_value.scheduler_info.side_effect = Exception()

        mock_cluster = mock.MagicMock()

        manager = ClusterManager('192.168.55.55:8786', mock_cluster)

        with self.assertRaises(Exception):
            manager.find_stale_workers()

    @mock.patch('cwt_kubernetes.cluster_manager.Client')
    def test_find_stale_workers_missing_entry(self, mock_client):
        mock_client.return_value.scheduler_info.return_value = {}

        mock_cluster = mock.MagicMock()

        manager = ClusterManager('192.168.55.55:8786', mock_cluster)

        with self.assertRaises(KeyError):
            manager.find_stale_workers()

    @mock.patch('cwt_kubernetes.cluster_manager.Client')
    def test_find_stale_workers(self, mock_client):
        mock_client.return_value.scheduler_info.return_value = {
            'workers': {
                'tcp://192.168.0.1': {
                    'host': '192.168.0.1',
                    'metrics': {
                        'executing': 0,
                        'in_memory': 0,
                        'ready': 0,
                        'in_flight': 0,
                    }
                },
                'tcp://192.168.0.2': {
                    'host': '192.168.0.2',
                    'metrics': {
                        'executing': 0,
                        'in_memory': 0,
                        'ready': 0,
                        'in_flight': 0,
                    }
                },
                'tcp://192.168.0.3': {
                    'host': '192.168.0.3',
                    'metrics': {
                        'executing': 0,
                        'in_memory': 4,
                        'ready': 0,
                        'in_flight': 0,
                    }
                }
            }
        }

        mock_cluster = mock.MagicMock()

        manager = ClusterManager('192.168.55.55:8786', mock_cluster)

        workers = manager.find_stale_workers()

        expected_workers = ['192.168.0.1', '192.168.0.2']

        self.assertEqual(workers, expected_workers)
