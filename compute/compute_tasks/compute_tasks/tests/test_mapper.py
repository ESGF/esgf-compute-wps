import json
import re
import tempfile
import yaml

import pytest

from compute_tasks.mapper import Mapper
from compute_tasks.mapper import MatchNotFoundError

POD = """
apiVersion: v1
kind: Pod
metadata:
    name: test
spec:
  containers:
  - name: test
"""

DEPLOYMENT = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test
spec:
  replicas: 1
  selector:
    matchLabels:
      app: test
  template:
    metadata:
      labels:
        app: test
    spec:
      containers:
      - name: test
"""

CONFIGMAP = """
apiVersion: apps/v1
kind: ConfigMap
"""

TEST_CONFIG = {
    'mounts': [
        {
            'type': 'hostpath',
            'path': '/data',
        },
        {
            'type': 'hostpath',
            'path': '/exports/data',
        },
    ],
    'mapping': {
        'user_data': '/data/user/data',
        'public_data': '/exports/data/public_data',
    }
}

def test_patch_k8s_resource_invalid_type():
    resource = yaml.load(CONFIGMAP)

    map = Mapper(TEST_CONFIG['mounts'], Mapper.convert_mapping(TEST_CONFIG['mapping']))

    with pytest.raises(Exception):
        map.patch_k8s_resource(resource)

@pytest.mark.parametrize('data,spec_func', [
    (POD, lambda x: x['spec']),
    (DEPLOYMENT, lambda x: x['spec']['template']['spec']),
])
def test_patch_k8s_resource(data, spec_func):
    resource = yaml.load(data)

    map = Mapper(TEST_CONFIG['mounts'], Mapper.convert_mapping(TEST_CONFIG['mapping']))

    patched = map.patch_k8s_resource(resource)

    spec = spec_func(patched)

    volumes = spec['volumes']
    volumeMounts = spec['containers'][0]['volumeMounts']

    assert len(volumes) == 2
    assert len(volumeMounts) == 2

    volumes = sorted(volumes, key=lambda x: x['name'])
    volumeMounts = sorted(volumeMounts, key=lambda x: x['name'])

    for x, y in zip(volumes, volumeMounts):
        assert x['name'] == y['name']
        assert x['hostPath']['path'] == y['mountPath']

def test_find_match_no_match(mocker):
    map = Mapper(TEST_CONFIG['mounts'], Mapper.convert_mapping(TEST_CONFIG['mapping']))

    with pytest.raises(MatchNotFoundError):
        map.find_match('https://data.io/thredds/cmip5/miroc/3hr/test1.nc')

def test_find_match_does_not_exist(mocker):
    map = Mapper(TEST_CONFIG['mounts'], Mapper.convert_mapping(TEST_CONFIG['mapping']))

    with pytest.raises(MatchNotFoundError):
        map.find_match('https://data.io/thredds/user_data/miroc/3hr/test1.nc')

def test_find_match(mocker):
    mocker.patch('os.path.exists', return_value=True)

    map = Mapper(TEST_CONFIG['mounts'], Mapper.convert_mapping(TEST_CONFIG['mapping']))

    path = map.find_match('https://data.io/thredds/user_data/miroc/3hr/test1.nc')

    assert path == '/data/user/data/miroc/3hr/test1.nc'

def test_covert_mapping():
    matchers = Mapper.convert_mapping(TEST_CONFIG['mapping'])

    assert all([isinstance(x[0], re.Pattern) for x in matchers])
    assert all([isinstance(x[1], str) for x in matchers])

def test_from_config():
    temp = tempfile.TemporaryFile()
    temp.write(json.dumps(TEST_CONFIG).encode())
    temp.seek(0)

    map = Mapper.from_config(temp.name)

    assert len(map.mounts) == 2
    assert len(map.matchers) == 2
