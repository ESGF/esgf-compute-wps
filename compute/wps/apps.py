from __future__ import unicode_literals

import celery
import django
import logging
import os

from django import db
from django.apps import AppConfig

class WpsConfig(AppConfig):
    name = 'wps'

    def ready(self):
        pass
