#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import glob
import json
import importlib
import os
import re
import types

import cwt
from celery import signals
from celery import Celery
from kombu import serialization
from kombu import Exchange
from kombu import Queue

def default(obj):
    if isinstance(obj, slice):
        data = {
            '__type': 'slice',
            'start': obj.start,
            'stop': obj.stop,
            'step': obj.step,
        }
    elif isinstance(obj, cwt.Variable):
        data = {
            'data': obj.parameterize(),
            '__type': 'variable',
        }
    elif isinstance(obj, cwt.Domain):
        data = {
            'data': obj.parameterize(),
            '__type': 'domain',
        }
    elif isinstance(obj, cwt.Process):
        data = {
            'data': obj.parameterize(),
            '__type': 'process',
        }
    elif isinstance(obj, datetime.timedelta):
        data = {
            'data': {
                'days': obj.days,
                'seconds': obj.seconds,
                'microseconds': obj.microseconds,
            },
            '__type': 'timedelta',
        }
    elif isinstance(obj, types.FunctionType):
        data = {
            'data': {
                'module': obj.__module__,
                'name': obj.__name__,
            },
            '__type': 'function',
        }
    else:
        raise TypeError(type(obj))

    return data

def object_hook(obj):
    if '__type' not in obj:
        return byteify(obj)

    if obj['__type'] == 'slice':
        data = slice(obj['start'], obj['stop'], obj['step'])
    elif obj['__type'] == 'variable':
        data = cwt.Variable.from_dict(byteify(obj['data']))
    elif obj['__type'] == 'domain':
        data = cwt.Domain.from_dict(byteify(obj['data']))
    elif obj['__type'] == 'process':
        data = cwt.Process.from_dict(byteify(obj['data']))
    elif obj['__type'] == 'timedelta':
        kwargs = {
            'days': obj['data']['days'],
            'seconds': obj['data']['seconds'],
            'microseconds': obj['data']['microseconds'],
        }

        data = datetime.timedelta(**kwargs)
    elif obj['__type'] == 'function':
        data = importlib.import_module(obj['data']['module'])

        data = getattr(data, obj['data']['name'])

    return data

def byteify(data):
    if isinstance(data, dict):
        return dict((byteify(x), byteify(y)) for x, y in data.iteritems())
    elif isinstance(data, list):
        return list(byteify(x) for x in data)
    elif isinstance(data, unicode):
        return data.encode('utf-8')
    else:
        return data

encoder = lambda x: json.dumps(x, default=default)
decoder = lambda x: json.loads(x, object_hook=object_hook)

serialization.register('cwt_json', encoder, decoder, 'application/json')

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'compute.settings')

app = Celery(
    'compute',
    backend = os.getenv('CELERY_BACKEND', 'redis://0.0.0.0'),
    broker = os.getenv('CELERY_BROKER', 'redis://0.0.0.0'),
)

app.config_from_object('django.conf:settings', namespace='CELERY')

ingress_exchange = Exchange('ingress', type='topic')
priority_exchange = Exchange('priority', type='topic')

app.conf.task_queues = [
    Queue('ingress', ingress_exchange, routing_key='ingress'),
    Queue('priority.high', priority_exchange, routing_key='high'),
    Queue('priority.low', priority_exchange, routing_key='low'),
]

app.conf.event_serializer = 'cwt_json'
app.conf.result_serializer = 'cwt_json'
app.conf.task_serializer = 'cwt_json'

app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
