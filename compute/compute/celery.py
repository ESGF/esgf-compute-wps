#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import kombu
import os
import re

from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'compute.settings')

app = Celery(
    'compute',
    backend = os.getenv('CELERY_BACKEND', 'redis://0.0.0.0'),
    broker = os.getenv('CELERY_BROKER', 'redis://0.0.0.0'),
)

app.config_from_object('django.conf:settings', namespace='CELERY')

ingress_exchange = kombu.Exchange('ingress', type='topic')
priority_exchange = kombu.Exchange('priority', type='topic')

app.conf.task_queues = [
    kombu.Queue('ingress', ingress_exchange, routing_key='ingress'),
    kombu.Queue('priority.high', priority_exchange, routing_key='high'),
    kombu.Queue('priority.low', priority_exchange, routing_key='low'),
]

app.conf.task_routes = {
    'wps.tasks.ingress.*': {
        'queue': 'ingress',
        'routing_key': 'ingress',
    }
}

app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
