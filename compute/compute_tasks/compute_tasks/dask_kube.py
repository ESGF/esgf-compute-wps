import os
import time
import logging

logging.basicConfig(level=logging.DEBUG)

MAXIMUM = os.environ.get('MAXIMUM_WORKERS', 2)
NAMESPACE = os.environ.get('NAMESPACE', 'default')

def main():
    from dask_kubernetes import KubeCluster
    from kubernetes import client
    from kubernetes import config

    config.load_incluster_config()

    core = client.CoreV1Api()

    cluster = KubeCluster()

    cluster.adapt(minimum=0, maximum=MAXIMUM)

    while True:
        time.sleep(2)

    namespace = client.V1Namespace()
    namespace.metadata = client.V1ObjectMeta(labels={'compute.io/cleanup': 'now'})

    core.patch_namespace(NAMESPACE, namespace)
