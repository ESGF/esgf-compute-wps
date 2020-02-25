import os
import time
import logging

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

    while True:
        time.sleep(2)
