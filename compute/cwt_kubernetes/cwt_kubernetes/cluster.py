import json
import logging
import uuid

import yaml
from kubernetes import client
from kubernetes import config

api_client = client.ApiClient()


class FakeResponse(object):
    def __init__(self, data):
        self.data = data


class Cluster(object):
    def __init__(self, namespace, pod=None):
        self.namespace = namespace

        self.pod = pod

        config.load_incluster_config()

        self.core = client.CoreV1Api()

    @classmethod
    def from_yaml(cls, namespace, file_path):
        with open(file_path) as infile:
            data = yaml.safe_load(infile)

        pod = api_client.deserialize(FakeResponse(json.dumps(data)), client.V1Pod)

        return cls(namespace, pod)

    def scale_up(self, n):
        logging.info('Scaling up dask workers by %r workers', n)

        for x in range(n):
            name = 'dask-worker-{!s}'.format(str(uuid.uuid4())[:13])

            self.pod.metadata.name = name

            try:
                self.core.create_namespaced_pod(self.namespace, self.pod)
            except client.rest.ApiException as e:
                logging.error('Error creating pod %r', e)

                raise

    def scale_down(self, workers):
        pods = self.core.list_namespaced_pod(self.namespace)

        logging.info('Listed %r pods %r', len(pods.items), [x for x in pods.items])

        to_delete = [x for x in pods.items if x.status.pod_ip in workers]

        logging.info('Marked %r pods to be deleted %r', len(to_delete), [x for x in to_delete])

        if len(to_delete) == 0:
            return

        for pod in to_delete:
            logging.info('Deleting pod %r', pod.metadata.name)

            try:
                self.core.delete_namespaced_pod(pod.metadata.name, self.namespace)
            except client.rest.ApiException as e:
                logging.error('Error deleting pod %r', e)
