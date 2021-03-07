import logging

from jinja2 import DebugUndefined
from jinja2 import Template
from kubernetes import client
from kubernetes import config

logger = logging.getLogger(__name__)

class KubernetesAllocator(object):
    def __init__(self):
        config.load_incluster_config()

        self.core = client.CoreV1Api()

        self.apps = client.AppsV1Api()

        self.extensions = client.ExtensionsV1beta1Api()

    def create_pod(self, namespace, body, **kwargs):
        return self.core.create_namespaced_pod(namespace, body, **kwargs)

    def list_pods(self, namespace, label_selector, **kwargs):
        return self.core.list_namespaced_pod(namespace, label_selector=label_selector, **kwargs)

    def create_deployment(self, namespace, body, **kwargs):
        return self.apps.create_namespaced_deployment(namespace, body, **kwargs)

    def create_service(self, namespace, body, **kwargs):
        return self.core.create_namespaced_service(namespace, body, **kwargs)

    def create_ingress(self, namespace, body, **kwargs):
        return self.extensions.create_namespaced_ingress(namespace, body, **kwargs)

    def create_config_map(self, namespace, body, **kwargs):
        return self.core.create_namespaced_config_map(namespace, body, **kwargs)

    def delete_resources(self, namespace, label_selector, **kwargs):
        api_mapping = {
            'pod': self.core,
            'deployment': self.apps,
            'service': self.core,
            'ingress': self.extensions,
            'config_map': self.core,
        }

        for name, api in api_mapping.items():
            list_name = f'list_namespaced_{name!s}'
            delete_name = f'delete_namespaced_{name!s}'

            output = getattr(api, list_name)(namespace, label_selector=label_selector, **kwargs)

            logger.info(f'Removing {len(output.items)!r} {name!s}')

            for x in output.items:
                getattr(api, delete_name)(x.metadata.name, namespace, **kwargs)

    def create_resources(self, request, namespace, labels, service_account_name, image_pull_secret, **kwargs):
        for item in request:
            template = Template(item, undefined=DebugUndefined)

            config = {
                'image_pull_secret': image_pull_secret,
                'labels': [f'{x}: {y}' for x, y in labels.items()],
            }

            rendered_item = template.render(**config)

            yaml_data = yaml.safe_load(rendered_item)

            try:
                yaml_data['metadata']['labels'].update(labels)
            except KeyError:
                yaml_data['metadata'].update({
                    'labels': labels
                })

            kind = yaml_data['kind']

            logger.info(f'Allocating {kind!r} with labels {yaml_data["metadata"]["labels"]!r}')

            if kind == 'Pod':
                yaml_data['spec']['serviceAccountName'] = service_account_name
                yaml_data['spec']['imagePullSecrets'] = [
                    {'name': image_pull_secret},
                ]

                self.create_pod(namespace, yaml_data)
            elif kind == 'Deployment':
                yaml_data['spec']['template']['spec']['serviceAccountName'] = service_account_name
                yaml_data['spec']['template']['spec']['imagePullSecrets'] = [
                    {'name': image_pull_secret},
                ]

                self.create_deployment(namespace, yaml_data)
            elif kind == 'Service':
                self.create_service(namespace, yaml_data)
            elif kind == 'Ingress':
                self.create_ingress(namespace, yaml_data)
            elif kind == 'ConfigMap':
                self.create_config_map(namespace, yaml_data)
            else:
                raise Exception('Requested an unsupported resource')

