import os
import logging
import threading
import time

from kubernetes import client
from kubernetes import config

from compute_provisioner import allocator

logger = logging.getLogger('provisioner.kube_cluster')


class KubeCluster:
    def __init__(self, namespace, dry_run, ignore_lifetime, **kwargs):
        self.namespace = namespace

        self.dry_run = dry_run

        self.ignore_lifetime = ignore_lifetime

        self.k8s = allocator.KubernetesAllocator()

    def check_resources(self, resources):
        for x in resources:
            expire = resources[x]

            logger.info(f'Checking resource group {x}')

            label_selector = f'compute.io/resource-group={x!s}'

            if expire is None:
                self.k8s.delete_resources(self.namespace, label_selector)
            else:
                expire = float(expire)

                if expire < time.time() or self.ignore_lifetime:
                    self.k8s.delete_resources(self.namespace, label_selector)

                    del resources[x]

        logger.info('Removing rogue resources')

        key_list = ', '.join([x for x in resources])

        rogue_selector = f'compute.io/resource-group,compute.io/resource-group notin ({key_list!s})'

        self.k8s.delete_resources(self.namespace, rogue_selector)

        complete_selector = f'compute.io/resource-group'

        pods = self.k8s.list_pods(self.namespace, complete_selector)

        logger.info(f'Checking {len(pods.items)} resource groups of end of life phase')

        resource_keys = []

        for x in pods.items:
            eol_phase = x.status.phase in ('Succeeded', 'Failed', 'Unknown')
            work_done = x.metadata.labels.get('compute.io/state', '') == 'Done'

            if eol_phase or work_done:
                resource_keys.append(x.metadata.labels['compute.io/resource-group'])

                logger.info(f'Found resource group {resource_keys[-1]} with eol condition')

                del resources[resource_keys[-1]]

                logger.debug(f'Removed {count} entries in redis')

        eol_resource_keys = ', '.join(resource_keys)

        eol_selector = f'compute.io/resource-group,compute.io/resource-group in ({eol_resource_keys!s})'

        self.k8s.delete_resources(self.namespace, eol_selector)
