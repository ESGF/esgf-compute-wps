import logging
import threading
import time

import redis
from kubernetes import client
from kubernetes import config

from compute_provisioner.provisioner import KubernetesAllocator

logger = logging.getLogger('provisioner.kube_cluster')


class KubeCluster(threading.Thread):
    def __init__(self, redis_host, namespace, timeout, dry_run, ignore_lifetime, **kwargs):
        super(KubeCluster, self).__init__(target=self.monitor)

        self.redis_host = redis_host

        self.namespace = namespace

        self.timeout = timeout

        self.dry_run = dry_run

        self.ignore_lifetime = ignore_lifetime

        self.k8s = KubernetesAllocator()

        self.redis = redis.Redis(self.redis_host, db=1)

    def check_resources(self):
        keys = self.redis.hkeys('resource')

        for x in keys:
            expire = self.redis.hget('resource', x)

            label_selector = f'compute.io/resource-group={x!s}'

            if expire is None:
                self.k8s.delete_resources(self.namespace, label_selector)
            else:
                expire = float(expire.decode())

                if expire < time.time() or self.ignore_lifetime:
                    self.k8s.delete_resources(self.namespace, label_selector)

                    self.redis.hdel('resource', key)

        key_list = ', '.join(keys)

        rogue_selector = f'compute.io/resource-group notin ({key_list!s})'

        self.k8s.delete_resources(self.namespace, rogue_selector)

    def monitor(self):
        while True:
            self.check_resources()

            time.sleep(self.timeout)

def main():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    parser.add_argument('--redis-host', help='Redis host', required=True)

    parser.add_argument('--namespace', help='Kubernetes namespace to monitor', default='default')

    parser.add_argument('--timeout', help='Resource monitor timeout', type=int, default=30)

    parser.add_argument('--dry-run', help='Does not actually remove resources', action='store_true')

    parser.add_argument('--ignore-lifetime', help='Ignores lifetime', action='store_true')

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    monitor = KubeCluster(**vars(args))

    monitor.start()

    monitor.join()

if __name__ == '__main__':
    main()
