#! /usr/bin/env python

import argparse
import os
import sys

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.insert(0, base_path)

os.environ['WPS_NO_INIT'] = ''
os.environ['DJANGO_SETTINGS_MODULE'] = 'compute.settings'

import django
from django.db.models import fields
from django.conf import settings

django.setup()

from wps import models

TYPE_MAP = {
        fields.CharField: str,
        fields.IntegerField: int,
        fields.PositiveIntegerField: int,
        }

def add_model(args):
    model = args.model()

    for f in model._meta.get_fields():
        if hasattr(args, f.name):
            value = getattr(args, f.name)

            if value is not None:
                setattr(model, f.name, value)

    model.save()

def find_models_by_criteria(args):
    if args.match is None or len(args.match) == 0:
        raise Exception('Must pass list of key=value to --match')

    fields = args.model._meta.get_fields()

    fields = [x.name for x in fields]

    criteria = {}

    for c in args.match:
        k, v = c.split('=')

        if k not in fields:
            raise Exception('Model %s does not have field %s' % (args.model.__class__, k))
        else:
            criteria[k] = v

    models = args.model.objects.filter(**criteria)

    return models

def remove_model(args):
    models = find_models_by_criteria(args)

    for m in models:
        m.delete()

def update_model(args):
    models = find_models_by_criteria(args)

    for m in models:
        for f in m._meta.get_fields():
            if hasattr(args, f.name):
                value = getattr(args, f.name)

                if value is not None:
                    setattr(m, f.name, value)

        m.save()

def list_model(args, key):
    models = args.model.objects.all()

    for m in models:
        print key, ':', getattr(m, key)

        for f in m._meta.get_fields():
            if hasattr(m, f.name):
                value = getattr(m, f.name)

                if isinstance(f, django.db.models.fields.TextField):
                    value = value[:100]

                print '\t', f.name, '=', value

def process_cmd(args):
    if args.action == 'add':
        add_model(args)
    elif args.action == 'remove':
        remove_model(args)
    elif args.action == 'update':
        update_model(args)
    elif args.action == 'list':
        list_model(args, args.key)

def create_sub_parser(model, subparsers, actions, key=None):
    name = model.__name__.lower()

    parser = subparsers.add_parser(name)
    parser.set_defaults(func=process_cmd, model=model, key=key)
    parser.add_argument('-a', '--action', choices=actions, required=True)

    if 'remove' in actions or 'update' in actions:
        parser.add_argument('-m', '--match', nargs='*')

    for f in model._meta.get_fields():
        if f.__class__ in TYPE_MAP:
            parser.add_argument('--{0}'.format(f.name), type=TYPE_MAP[f.__class__])

def create_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    create_sub_parser(models.Instance,
            subparsers,
            ['add', 'remove', 'update', 'list'],
            'host')

    create_sub_parser(models.Server,
            subparsers,
            ['add', 'remove', 'update', 'list'],
            'host')

    create_sub_parser(models.Job,
            subparsers,
            ['list'],
            'id')

    create_sub_parser(models.Process,
            subparsers,
            ['list'],
            'id')


    return parser

parser = create_parser()

args = parser.parse_args()

args.func(args)
