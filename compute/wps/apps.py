from __future__ import unicode_literals

import logging
import os

import celery
import django

from django.apps import AppConfig

class WpsConfig(AppConfig):
    name = 'wps'

    def ready(self):
        # Need to import after app is ready
        from wps import models
        from wps import tasks

        logger = logging.getLogger(__name__)

        if os.getenv('WPS_NO_INIT') is not None:
            return

        logger.info('Initializing server')

        try:
            models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            logger.info('Default server does not exist')

            server = models.Server(host='default')

            server.save()
        except django.db.utils.ProgrammingError:
            logger.info('Database has not been initialized yet')

            return

        servers = models.Server.objects.all()

        # Queue capabilities tasks for uninitialized servers
        celery.group(tasks.capabilities.s(x.id)
                for x in servers if x.capabilities == '')()
