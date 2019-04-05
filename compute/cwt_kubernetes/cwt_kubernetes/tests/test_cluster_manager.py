import mock
import unittest

from cwt_kubernetes.cluster_manager import ClusterManager


class TestClusterManager(unittest.TestCase):
    @mock.patch('cwt_kubernetes.cluster_manager.Client')
    def test_scale_down_workers(self, mock_client):
        mock_cluster = mock.MagicMock()

        mock_client.return_value.retire_workers.return_value = {
            'worker1': {
                'host': '192.168.0.1',
            },
            'worker2': {
                'host': '192.168.0.2',
            }
        }

        manager = ClusterManager('192.168.111.111:8786', mock_cluster)

        manager.scale_down_workers()

        mock_cluster.scale_down.assert_called_with(['192.168.0.1', '192.168.0.2'])

    @mock.patch('cwt_kubernetes.cluster_manager.Client')
    def test_get_pods_to_kill(self, mock_client):
        mock_cluster = mock.MagicMock()

        mock_client.return_value.retire_workers.return_value = {
            'worker1': {
                'host': '192.168.0.1',
            },
            'worker2': {
                'host': '192.168.0.2',
            }
        }

        manager = ClusterManager('192.168.111.111:8786', mock_cluster)

        workers = manager.get_pods_to_kill()

        self.assertEqual(len(workers), 2)

        self.assertEqual(workers, ['192.168.0.1', '192.168.0.2'])

    @mock.patch('cwt_kubernetes.cluster_manager.Client')
    def test_get_pods_to_kill_empty(self, mock_client):
        mock_cluster = mock.MagicMock()

        manager = ClusterManager('192.168.111.111:8786', mock_cluster)

        workers = manager.get_pods_to_kill()

        self.assertEqual(len(workers), 0)
