#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import json
import os
import re


import cwt
from kombu import serialization
from kombu import Exchange
from kombu import Queue

from celery import Celery

def default(obj):
    if isinstance(obj, slice):
        return {
            '__type': 'slice',
            'start': obj.start,
            'stop': obj.stop,
            'step': obj.step,
        }
    elif isinstance(obj, cwt.Domain):
        data = {
            'data': obj.parameterize(),
            '__type': 'domain',
        }

        return data
    else:
        raise TypeError(type(obj))

def object_hook(obj):
    if '__type' in obj and obj['__type'] == 'slice':
        return slice(obj['start'], obj['stop'], obj['step'])
    elif '__type' in obj and obj['__type'] == 'domain':
        return cwt.Domain.from_dict(byteify(obj['data']))

    return obj

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
