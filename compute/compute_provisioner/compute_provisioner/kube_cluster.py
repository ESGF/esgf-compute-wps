import logging
import threading
import time

import redis
from kubernetes import client
from kubernetes import config

logger = logging.getLogger('provisioner.kube_cluster')


class KubeCluster(threading.Thread):
    def __init__(self, redis_host, namespace, timeout, dry_run, ignore_lifetime, **kwargs):
        super(KubeCluster, self).__init__(target=self.monitor)

        self.redis_host = redis_host

        self.namespace = namespace

        self.timeout = timeout

        self.dry_run = dry_run

        self.ignore_lifetime = ignore_lifetime

        config.load_incluster_config()

        self.core = client.CoreV1Api()

        self.apps = client.AppsV1Api()

        self.exts = client.ExtensionsV1beta1Api()

        self.redis = redis.Redis(self.redis_host, db=1)

    def check_rogue_expired_resources(self, kind, selector, resource, delete_func):
        logger.info('Checking %r %r resources', len(resource.items), kind)

        for x in resource.items:
            key = x.metadata.labels[selector]

            expire = self.redis.hget('resource', key)

            if expire is None:
                logger.info('Removing rogue resource %r %r', kind, x.metadata.name)

                if not self.dry_run:
                    delete_func(x.metadata.name, self.namespace)

                    logger.info('Removed %r %r', kind, x.metadata.name)
            else:
                expire = float(expire.decode())

                logger.info('Resource %r set to expire in %r', x.metadata.name, expire-time.time())

                if expire < time.time() or self.ignore_lifetime:
                    logger.info('Removing expired resource %r %r', kind, x.metadata.name)

                    if not self.dry_run:
                        delete_func(x.metadata.name, self.namespace)

                        logger.info('Removed %r %r', kind, x.metadata.name)

                        self.redis.hdel('resource', key)
                else:
                    logger.debug('Found validate resource %r %r', kind, x.metadata.name)

    def remove_resource(self, resource, delete_func):
        for x in resource['items']:
            try:
                delete_func(x['metadata']['name'], self.namespace)
            except Exception:
                # Resource should get cleaned up
                logger.exception('Failed to delete resource')

    def check_resources(self):
        keys_to_remove = []

        for resource_uuid, expire in self.redis.hscan_iter('resource'):
            logger.info(f'Checking resource uuid {resource_uuid!r} expire {expire!r}')

            selector = 'app.kubernetes.io/resource-group-uuid={!s}'.format(resource_uuid)

            expire = float(expire)

            if time.time() <= expire:
                logger.info('Resource not expired')

                continue

            resource = self.apps.list_namespaced_deployment(self.namespace, label_selector=selector)

            self.remove_resource(resource, self.apps.delete_namespaced_deployment)

            resource = self.core.list_namespaced_pod(self.namespace, label_selector=selector)

            self.remove_resource(resource, self.core.delete_namespaced_pod)

            resource = self.core.list_namespaced_service(self.namespace, label_selector=selector)

            self.remove_resource(resource, self.core.delete_namespaced_service)

            resource = self.exts.list_namespaced_ingress(self.namespace, label_selector=selector)

            self.remove_resource(resource, self.exts.delete_namespaced_ingress)

            keys_to_remove.append(resource_uuid)

        logger.info(f'Removing keys {keys_to_remove!r}')

        if len(keys_to_remove) > 0:
            with self.redis.lock('resource'):
                self.redis.hdel(*keys_to_remove)

        logger.info('Done checking for resources')

    def validate_existing_resources(self):
        selector = 'app.kubernetes.io/resource-group-uuid'

        pods = self.core.list_namespaced_pod(self.namespace, label_selector=selector)

        self.check_rogue_expired_resources('Pod', selector, pods, self.core.delete_namespaced_pod)

        deployments = self.apps.list_namespaced_deployment(self.namespace, label_selector=selector)

        self.check_rogue_expired_resources('Deployment', selector, deployments, self.apps.delete_namespaced_deployment)

        services = self.core.list_namespaced_service(self.namespace, label_selector=selector)

        self.check_rogue_expired_resources('Service', selector, services, self.core.delete_namespaced_service)

        ingresses = self.exts.list_namespaced_ingress(self.namespace, label_selector=selector)

        self.check_rogue_expired_resources('Ingress', selector, ingresses, self.exts.delete_namespaced_ingress)

    def monitor(self):
        self.validate_existing_resources()

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
