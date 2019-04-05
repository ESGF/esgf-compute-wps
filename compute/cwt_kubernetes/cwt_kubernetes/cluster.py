import json
import logging
import uuid

import yaml
from kubernetes import client
from kubernetes import config

api_client = client.ApiClient()

CLEANUP_PHASES = ('Completed', 'Failed')


class FakeResponse(object):
    def __init__(self, data):
        self.data = data


class Cluster(object):
    def __init__(self, namespace, pod_template=None):
        self.namespace = namespace

        self.pod_template = pod_template

        self.pod_template.metadata.labels = {}
        self.pod_template.metadata.labels['app'] = 'dask'
        self.pod_template.metadata.labels['component'] = 'dask-worker'

        config.load_incluster_config()

        self.core_api = client.CoreV1Api()

    @classmethod
    def from_yaml(cls, namespace, file_path):
        with open(file_path) as infile:
            data = yaml.safe_load(infile)

        pod_template = api_client.deserialize(FakeResponse(json.dumps(data)), client.V1Pod)

        return cls(namespace, pod_template)

    def get_pods(self):
        selector = ','.join('{!s}={!s}'.format(x, y) for x, y in list(self.pod_template.metadata.labels.items()))

        try:
            pods = self.core_api.list_namespaced_pod(self.namespace, label_selector=selector)
        except client.rest.ApiException:
            logging.error('Error listing pods for namespace %r with labels %r', self.namespace, selector)

            raise

        logging.info('Retrieved %r pods', len(pods.items))

        return pods

    def delete_pod(self, pod):
        logging.info('Delete pod %r', pod.metadata.name)

        try:
            self.core_api.delete_namespaced_pod(pod.metadata.name, self.namespace)
        except client.rest.ApiException as e:
            logging.error('Failed to delete pod %r: %r', pod.metadata.name, e)

    def cleanup_pods(self, pods):
        to_delete = [x for x in pods.items if x.status.phase in CLEANUP_PHASES]

        logging.info('Marking %r pods for deletion', len(to_delete))

        for x in to_delete:
            self.delete_pod(x)

        return [x for x in pods.items if x.status.phase not in CLEANUP_PHASES]

    def scale_up(self, n):
        pods = self.get_pods()

        self.cleanup_pods(pods)

        logging.info('Scaling up dask workers by %r workers', n)

        for x in range(n):
            name = 'dask-worker-{!s}'.format(str(uuid.uuid4())[:13])

            self.pod_template.metadata.name = name

            try:
                self.core_api.create_namespaced_pod(self.namespace, self.pod_template)
            except client.rest.ApiException as e:
                logging.error('Error creating pod %r', e)

                raise

    def scale_down(self, workers):
        pods = self.get_pods()

        pods = self.cleanup_pods(pods)

        logging.info('Mapping %r pods to %r candidates', len(pods), len(workers))

        to_delete = [x for x in pods if x.status.pod_ip in workers]

        if len(to_delete) == 0:
            logging.info('No pods marked for deleteion')

            return

        logging.info('Marked %r pods for deletion', len(to_delete))

        for x in to_delete:
            self.delete_pod(x)
