#! /usr/bin/env python

from __future__ import absolute_import

from celery import Celery

app = Celery('wps',
        broker='redis://localhost',
        backend='redis://localhost',
        include=['wps.tasks'])

if __name__ == '__main__':
    app.start()
