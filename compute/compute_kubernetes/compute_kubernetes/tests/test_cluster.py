import mock
import unittest

from kubernetes import client

from compute_kubernetes.cluster import Cluster


class TestCluster(unittest.TestCase):

    def create_pod(self, phase):
        pod = mock.MagicMock()

        pod.status.phase = phase

        return pod

    @mock.patch('compute_kubernetes.cluster.config')
    def test_cleanup_pods(self, config):
        mock_pod = mock.MagicMock()

        cluster = Cluster('default', mock_pod)

        with mock.patch.object(cluster, 'delete_pod') as mock_delete:
            mock_pod_list = mock.MagicMock()
            mock_pod_list.items = [
                self.create_pod('Completed'),
                self.create_pod('Failed'),
                self.create_pod('Running'),
            ]

            result = cluster.cleanup_pods(mock_pod_list)

            mock_delete.assert_any_call(mock_pod_list.items[0])
            mock_delete.assert_any_call(mock_pod_list.items[1])

            self.assertEqual(len(result), 1)

    @mock.patch('compute_kubernetes.cluster.config')
    def test_delete_pod(self, config):
        mock_pod = mock.MagicMock()

        cluster = Cluster('default', mock_pod)

        with mock.patch.object(cluster, 'core_api') as mock_core:
            mock_pod_running = mock.MagicMock()
            mock_pod_running.metadata.name = 'dask-worker'

            cluster.delete_pod(mock_pod_running)

            mock_core.delete_namespaced_pod.assert_called_with('dask-worker', 'default')

    @mock.patch('compute_kubernetes.cluster.config')
    def test_get_pods(self, config):
        mock_pod = mock.MagicMock()

        cluster = Cluster('default', mock_pod)

        with mock.patch.object(cluster, 'core_api') as mock_core:
            cluster.get_pods()

            mock_core.list_namespaced_pod.assert_called_with('default', label_selector='app=dask,component=dask-worker')

    @mock.patch('compute_kubernetes.cluster.config')
    def test_scale_down_exception(self, config):
        mock_pod = mock.MagicMock()

        cluster = Cluster('default', mock_pod)

        namespaced_pods = mock.MagicMock()

        mock_pod1 = mock.MagicMock()
        mock_pod1.status.pod_ip = '192.168.0.2'
        mock_pod1.metadata.name = 'dask-worker-test2'

        mock_pod2 = mock.MagicMock()
        mock_pod2.status.pod_ip = '192.168.0.3'
        mock_pod2.metadata.name = 'dask-worker-test3'

        mock_pod3 = mock.MagicMock()
        mock_pod3.status.pod_ip = '192.168.0.4'
        mock_pod3.metadata.name = 'dask-worker-test4'

        type(namespaced_pods).items = mock.PropertyMock(return_value=[
            mock_pod1,
            mock_pod2,
            mock_pod3,
        ])

        with mock.patch.object(cluster, 'core_api') as mock_core:
            mock_core.list_namespaced_pod.return_value = namespaced_pods

            mock_core.delete_namespaced_pod.side_effect = [
                client.rest.ApiException,
                None
            ]

            cluster.scale_down(['192.168.0.4', '192.168.0.2'])

            mock_core.list_namespaced_pod.assert_called_with('default', label_selector='app=dask,component=dask-worker')

            mock_core.delete_namespaced_pod.assert_any_call('dask-worker-test2', 'default')

            mock_core.delete_namespaced_pod.assert_any_call('dask-worker-test4', 'default')

    @mock.patch('compute_kubernetes.cluster.config')
    def test_scale_down_empty(self, config):
        mock_pod = mock.MagicMock()

        cluster = Cluster('default', mock_pod)

        with mock.patch.object(cluster, 'core_api') as mock_core:
            cluster.scale_down([])

            mock_core.delete_namespaced_pod.assert_not_called()

    @mock.patch('compute_kubernetes.cluster.config')
    def test_scale_down(self, config):
        mock_pod = mock.MagicMock()

        cluster = Cluster('default', mock_pod)

        namespaced_pods = mock.MagicMock()

        mock_pod1 = mock.MagicMock()
        mock_pod1.status.pod_ip = '192.168.0.2'
        mock_pod1.metadata.name = 'dask-worker-test2'

        mock_pod2 = mock.MagicMock()
        mock_pod2.stauts.pod_ip = '192.168.0.3'
        mock_pod2.metadata.name = 'dask-worker-test3'

        type(namespaced_pods).items = mock.PropertyMock(return_value=[
            mock_pod1,
            mock_pod2,
        ])

        with mock.patch.object(cluster, 'core_api') as mock_core:
            mock_core.list_namespaced_pod.return_value = namespaced_pods

            cluster.scale_down(['192.168.0.1', '192.168.0.2'])

            mock_core.list_namespaced_pod.assert_called_with('default', label_selector='app=dask,component=dask-worker')

            mock_core.delete_namespaced_pod.assert_called_with('dask-worker-test2', 'default')

    @mock.patch('compute_kubernetes.cluster.config')
    @mock.patch('uuid.uuid4')
    def test_scale_up_exception(self, mock_uuid4, config):
        mock_pod = mock.MagicMock()

        mock_uuid4.side_effect = ['test1', 'test2']

        cluster = Cluster('default', mock_pod)

        with mock.patch.object(cluster, 'core_api') as mock_core:
            mock_core.create_namespaced_pod.side_effect = client.rest.ApiException()

            with self.assertRaises(client.rest.ApiException):
                cluster.scale_up(2)

    @mock.patch('compute_kubernetes.cluster.config')
    @mock.patch('uuid.uuid4')
    def test_scale_up(self, mock_uuid4, config):
        mock_pod = mock.MagicMock()

        mock_uuid4.side_effect = ['test1', 'test2']

        cluster = Cluster('default', mock_pod)

        with mock.patch.object(cluster, 'core_api') as mock_core:
            cluster.scale_up(2)

            mock_core.create_namespaced_pod.assert_any_call('default', mock_pod)

    @mock.patch('compute_kubernetes.cluster.config')
    def test_from_yaml(self, config):
        cluster = Cluster.from_yaml('default', 'tests/worker-spec.yml')

        self.assertIsNotNone(cluster.pod_template)
