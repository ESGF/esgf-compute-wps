#! /usr/bin/env python

import argparse
import os
import sys

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')

sys.path.insert(0, BASE_DIR)

import django
from django.db.models import fields
from django.conf import settings

settings.configure(
        DATABASES = {
            'default': {
                'ENGINE': 'django.db.backends.postgresql_psycopg2',
                'NAME': 'postgres',
                'USER': 'postgres',
                'PASSWORD': os.getenv('POSTGRES_PASSWORD', '1234'),
                'HOST': os.getenv('POSTGRES_HOST', 'localhost'),
                }
            },
        INSTALLED_APPS = (
            'wps',
            )
        )

django.setup()

from wps import models

TYPE_MAP = {
        fields.CharField: str,
        fields.BooleanField: bool,
        fields.IntegerField: int
        }

class ModelDoesNotExist(Exception):
    pass

class RequiredArgument(Exception):
    pass

def create_model(model, args, required=()):
    m = model()

    for f in model._meta.get_fields():
        if hasattr(args, f.name):
            value = getattr(args, f.name)

            if f.name in required and value is None:
                raise RequiredArgument(f.name)

            setattr(m, f.name, value)

    m.save()

def list_model(model, key, args):
    models = model.objects.all()

    for m in models:
        value = getattr(m, key)

        print key, ':', value

        for f in model._meta.get_fields():
            if hasattr(m, f.name):
                value = getattr(m, f.name)

                if hasattr(args, 'trunc') and isinstance(value, (unicode, str)):
                    value = str(value)

                    if getattr(args, 'trunc'):
                        value = value[0:100]

                print '\t', f.name, ':', value

def remove_model(model, **kwargs):
    try:
        m = model.objects.get(**kwargs)
    except model.DoesNotExist:
        raise ModelDoesNotExist('Could not find model matching {0}'.format(kwargs))
    else:
        m.delete()

def update_model(model, args, **kwargs):
    try:
        m = model.objects.get(**kwargs)
    except model.DoesNotExist:
        raise ModelDoesNotExist('Could not find model matching {0}'.format(kwargs))
    else:
        m.delete()
    
    for f in model._meta.get_fields():
        value = getattr(args, f.name)

        if value is not None:
            setattr(m, f.name, value)
    m.save()

def process_server(args):
    if args.action == 'add':
        create_model(models.Server, args, ('address'))
    elif args.action == 'list':
        list_model(models.Server, 'address', args)
    elif args.action == 'remove':
        remove_model(models.Server, address=args.address)
    elif args.action == 'update':
        update_model(models.Server, args, address=args.address)

def process_jobs(args):
    list_model(models.Job, 'id', args)

def create_parser():
    parent_parser = argparse.ArgumentParser(add_help=False)

    parser = argparse.ArgumentParser(add_help=False)
    subparsers = parser.add_subparsers()

    server = subparsers.add_parser('server', parents=[parent_parser])
    server.set_defaults(func=process_server)
    server.add_argument('action', choices=['add', 'list', 'remove', 'update'])

    for f in models.Server._meta.get_fields():
        if f.__class__ in TYPE_MAP:
            server.add_argument('--{0}'.format(f.name), type=TYPE_MAP[f.__class__])

    jobs = subparsers.add_parser('jobs', parents=[parent_parser])
    jobs.set_defaults(func=process_jobs)
    jobs.add_argument('--trunc', action='store_false')

    return parser

parser = create_parser()

args = parser.parse_args()

args.func(args)
