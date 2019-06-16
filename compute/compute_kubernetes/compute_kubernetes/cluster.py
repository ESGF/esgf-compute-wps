import logging
import os
import time

import redis

from kubernetes import client
from kubernetes import config

logger = logging.getLogger('compute_kubernetes.cluster')

NAMESPACE = os.environ.get('NAMESPACE', 'default')

CLEANUP_PHASES = ('Succeeded', 'Failed')


class Cluster(object):
    def __init__(self, redis_host, namespace, dry_run):
        self.redis_host = redis_host

        self.namespace = namespace

        self.dry_run = dry_run

        config.load_incluster_config()

        self.core = client.CoreV1Api()

        self.apps = client.AppsV1Api()

        self.exts = client.ExtensionsV1beta1Api()

        self.redis = redis.Redis(self.redis_host, db=1)

    def check_rogue_expired_resources(self, kind, selector, resource, delete_func):
        logger.info('Checking %r %r resources', len(resource.items), kind)

        for x in resource.items:
            key = '{!s}:{!s}:{!s}'.format(x.metadata.labels[selector], x.metadata.name, kind)

            expire = self.redis.hget('resource', key)

            if expire is None:
                logger.info('Removing rogue resource %r %r', kind, x.metadata.name)

                if not self.dry_run:
                    delete_func(x.metadata.name, NAMESPACE)

                    logger.info('Removed %r %r', kind, x.metadata.name)
            else:
                expire = float(expire.decode())

                if expire < time.time():
                    logger.info('Removing expired resource %r %r', kind, x.metadata.name)

                    if not self.dry_run:
                        delete_func(x.metadata.name, NAMESPACE)

                        logger.info('Removed %r %r', kind, x.metadata.name)

                        self.redis.hdel('resource', key)
                else:
                    logger.debug('Found validate resource %r %r', kind, x.metadata.name)

    def check_resources(self):
        for key, expire in self.redis.hscan_iter('resource'):
            logger.debug('Checking key %r expire %r', key, expire)

            uuid, name, kind = key.decode().split(':')

            selector = 'app.kubernetes.io/resource-group-uuid={!s}'.format(uuid)

            if kind == 'Pod':
                resource = self.core.list_namespaced_pod(NAMESPACE, label_selector=selector)

                delete_func = self.core.delete_namespaced_pod
            elif kind == 'Deployment':
                resource = self.apps.list_namespaced_deployment(NAMESPACE, label_selector=selector)

                delete_func = self.apps.delete_namespaced_deployment
            elif kind == 'Service':
                resource = self.core.list_namespaced_service(NAMESPACE, label_selector=selector)

                delete_func = self.core.delete_namespaced_service
            elif kind == 'Ingress':
                resource = self.exts.list_namespaced_ingress(NAMESPACE, label_selector=selector)

                delete_func = self.exts.delete_namespaced_ingress
            else:
                logger.error('Cannot handle resource %r kind', kind)

                continue

            expire = float(expire)

            if len(resource.items) == 0:
                logger.info('Resource not found, removing key %r', name)

                # Resource not found
                if not self.dry_run:
                    self.redis.hdel('resource', key)
            elif time.time() > expire:
                logger.info('Resource expired, removing %r %r', kind, name)

                # Resource expired
                if not self.dry_run:
                    delete_func(name, NAMESPACE)

                    self.redis.hdel('resource', key)

    def validate_existing_resources(self):
        selector = 'app.kubernetes.io/resource-group-uuid'

        pods = self.core.list_namespaced_pod(NAMESPACE, label_selector=selector)

        self.check_rogue_expired_resources('Pod', selector, pods, self.core.delete_namespaced_pod)

        deployments = self.apps.list_namespaced_deployment(NAMESPACE, label_selector=selector)

        self.check_rogue_expired_resources('Deployment', selector, deployments, self.apps.delete_namespaced_deployment)

        services = self.core.list_namespaced_service(NAMESPACE, label_selector=selector)

        self.check_rogue_expired_resources('Service', selector, services, self.core.delete_namespaced_service)

        ingresses = self.exts.list_namespaced_ingress(NAMESPACE, label_selector=selector)

        self.check_rogue_expired_resources('Ingress', selector, ingresses, self.exts.delete_namespaced_ingress)

    def monitor(self, timeout):
        self.validate_existing_resources()

        while True:
            self.check_resources()

            time.sleep(timeout)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Run Kubernetes monitor')

    parser.add_argument('--redis-host', help='Redis host')
    parser.add_argument('--namespace', help='Kubernetes namespace to monitor')
    parser.add_argument('--timeout', type=int, help='Seconds between scanning Kubernetes resources')
    parser.add_argument('--log-level', choices=logging._nameToLevel.keys(), default='INFO', help='Logging level')
    parser.add_argument('--dry-run', action='store_true', help='Dry run, no resources will be deleted')

    args = parser.parse_args()

    logging.basicConfig(level=logging._nameToLevel[args.log_level])

    redis_host = args.redis_host or os.environ['REDIS_HOST']

    namespace = args.namespace or os.environ.get('NAMESPACE', 'default')

    timeout = int(args.timeout or os.environ.get('TIMEOUT', 10))

    cluster = Cluster(redis_host, namespace, args.dry_run)

    cluster.monitor(timeout)
