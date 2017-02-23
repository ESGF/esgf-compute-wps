#! /usr/bin/env python

import argparse
import os
import sys

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')

sys.path.insert(0, BASE_DIR)

import django
from django.conf import settings

settings.configure(
        DATABASES = {
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
                }
            },
        INSTALLED_APPS = (
            'wps',
            )
        )

django.setup()

from wps import models

def add_update_server(args):
    server = models.Server.objects.get(address=args.address)

    if args.name is not None:
        server.name = args.name
    
    if args.is_wps is not None:
        server.is_wps = args.is_wps

    if args.queue_size is not None:
        server.queue_size = args.queue_size

    server.save()

def process_server(args):
    if args.action == 'add':
        add_update_server(args)
    elif args.action == 'remove':
        server = models.Server.objects.filter(address=args.address)

        if len(server) == 0:
            print 'No server exists with address equal to %s' % (args.address,)
        else:
            for s in server:
                s.delete()
    elif args.action == 'update':
        add_update_server(args)

def create_parser():
    parent_parser = argparse.ArgumentParser(add_help=False)

    parser = argparse.ArgumentParser(add_help=False)
    subparsers = parser.add_subparsers()

    server = subparsers.add_parser('server', parents=[parent_parser])
    server.add_argument('--action', default='add', choices=['add', 'remove', 'update'])
    server.add_argument('--name')
    server.add_argument('--is_wps', type=bool)
    server.add_argument('--queue_size', type=int)
    server.add_argument('address', type=str)
    server.set_defaults(func=process_server)

    return parser

parser = create_parser()

args = parser.parse_args()

args.func(args)
