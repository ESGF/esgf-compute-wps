#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import os

from celery import Celery
from kombu import serialization
from wps import helpers

serialization.register('cwt_json', helpers.encoder, helpers.decoder, 'application/json')

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'compute.settings')

app = Celery('compute')

app.config_from_object('compute.settings')

app.conf.event_serializer = 'cwt_json'
app.conf.result_serializer = 'cwt_json'
app.conf.task_serializer = 'cwt_json'

app.autodiscover_tasks(['wps.tasks.cdat',
                        'wps.tasks.edas',
                        'wps.tasks.metrics_',
                        'wps.tasks.job'])


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
