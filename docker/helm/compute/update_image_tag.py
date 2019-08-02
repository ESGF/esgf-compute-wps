#! /usr/bin/env python3

import argparse
from ruamel.yaml import YAML

parser = argparse.ArgumentParser()

parser.add_argument('file')
parser.add_argument('component', choices=('kubeMonitor', 'provisioner', 'nginx', 'wps', 'celery', 'thredds'))
parser.add_argument('value')

args = parser.parse_args()

yaml = YAML()

with open(args.file) as infile:
    data = yaml.load(infile)

data[args.component]['imageTag'] = args.value

with open(args.file, 'w') as outfile:
    yaml.dump(data, outfile)
