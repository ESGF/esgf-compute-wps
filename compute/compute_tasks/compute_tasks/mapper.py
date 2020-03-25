import hashlib
import json
import logging
import os
import re

logger = logging.getLogger(__name__)

class MatchNotFoundError(Exception):
    pass

class Mapper(object):
    def __init__(self, mounts, matchers):
        self.mounts = mounts
        self.matchers = matchers

    @staticmethod
    def convert_mapping(mapping):
        matchers = []

        logger.info(f'Converting {len(mapping)} mappings')

        for x, y in mapping.items():
            m = re.compile(f'^.*{ x }/(.*)$')

            matchers.append((m, y))

        return matchers

    @classmethod
    def from_config(cls, path):
        with open(path) as f:
            config = json.loads(f.read())

        mounts = config.get('mounts', [])

        mapping = config.get('mapping', {})

        logger.info(f'Loading mapping')

        matchers = cls.convert_mapping(mapping)

        return cls(mounts, matchers)

    def apply_volumes(self, resource):
        for m in self.mounts:
            uuid = hashlib.sha256(m['path'].encode()).hexdigest()[:8]

            logger.info(f'Applying volume {uuid!s}')

            if m['type'].lower() == 'hostpath':
                volume = {
                    'name': uuid,
                    'hostPath': {
                        'path': m['path'],
                    },
                }

                try:
                    resource['volumes'].append(volume)
                except KeyError:
                    resource.update({
                        'volumes': [volume,],
                    })

            volumeMount = {
                'name': uuid,
                'mountPath': m['path'],
            }

            try:
                resource['containers'][0]['volumeMounts'].append(volumeMount)
            except KeyError:
                resource['containers'][0].update({
                    'volumeMounts': [volumeMount,],
                })

        return resource

    def patch_k8s_resource(self, resource):
        if resource['kind'] == 'Pod':
            resource['spec'] = self.apply_volumes(resource['spec'])
        elif resource['kind'] == 'Deployment':
            resource['spec']['template']['spec'] = self.apply_volumes(resource['spec']['template']['spec'])
        else:
            raise Exception()

        return resource

    def find_match(self, url):
        m = None

        logger.info(f'Search for match to {url}')

        for expr, base in self.matchers:
            m = expr.match(url)

            if m is not None:
                break

        if m is None:
            logger.info(f'Did not find match')

            raise MatchNotFoundError()

        path = m.group(1)

        path = os.path.join(base, path)

        if not os.path.exists(path):
            logger.info(f'Local path {path} does not exist')

            raise MatchNotFoundError()

        logger.info(f'Replacing input with local {path}')

        return path
