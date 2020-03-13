import os
import time
import logging
import sys

logging.basicConfig(level=logging.DEBUG)

MAXIMUM = int(os.environ.get('MAXIMUM_WORKERS', 2))

def main():
    from dask_kubernetes import KubeCluster
    from kubernetes import client
    from kubernetes import config

    config.load_incluster_config()

    core = client.CoreV1Api()

    cluster = KubeCluster()

    cluster.adapt(minimum=0, maximum=MAXIMUM)

    timeout = time.time() + 120

    while True:
        # Workers took too long to spawn
        if time.time() > timeout:
            sys.exit(1)

        if len(cluster.scheduler.workers) > 0:
            break

        time.sleep(2)

    # Wait for workers to end
    while len(cluster.scheduler.workers) > 0:
        time.sleep(2)

    namespace = os.environ['NAMESPACE']
    name = os.environ['HOSTNAME']

    # Mark our pod as done
    core.patch_namespaced_pod(name, namespace, {"metadata": {"labels": {"compute.io/state": "Done"}}})

    # Wait to be killed
    while True:
        time.sleep(2)
