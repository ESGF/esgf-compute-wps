#! /usr/bin/env python

from __future__ import absolute_import

import os

from celery import Celery

app = Celery('wps',
        broker=os.getenv('CELERY_BROKER', 'redis://0.0.0.0'),
        backend=os.getenv('CELERY_BACKEND', 'redis://0.0.0.0'),
        include=['wps.tasks'])

if __name__ == '__main__':
    app.start()
