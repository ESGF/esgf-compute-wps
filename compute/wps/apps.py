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
        if os.getenv('WPS_INIT') is None:
            return

        # Need to import after app is ready
        from wps import models
        from wps import backends

        logger = logging.getLogger(__name__)

        logger.info('Initializing server')

        try:
            server = models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            logger.info('Default server does not exist')

            server = models.Server(host='default')

            server.save()
        except django.db.utils.ProgrammingError:
            logger.info('Database has not been initialized yet')

            return

        for backend in backends.Backend.registry.values():
            backend.populate_processes()
