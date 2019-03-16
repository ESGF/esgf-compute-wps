#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import os

from celery import Celery
from kombu import serialization
from kombu import Exchange
from kombu import Queue
from wps import helpers

serialization.register('cwt_json', helpers.encoder, helpers.decoder, 'application/json')

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'compute.settings')

app = Celery('compute')

app.config_from_object('compute.settings')

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
