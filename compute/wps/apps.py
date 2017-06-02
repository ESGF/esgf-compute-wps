from __future__ import unicode_literals

import celery
import django
import logging
import os

from django import db
from django.apps import AppConfig

from wps import wps_xml

class WpsConfig(AppConfig):
    name = 'wps'

    def ready(self):
        if os.getenv('WPS_INIT') is None:
            return

        # Need to import after app is ready
        from wps import models
        from wps import tasks
        from wps.processes import REGISTRY

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

        logger.info('Registering local processes')

        for i in REGISTRY.keys():
            logger.info('Registering "{}" process'.format(i))

            desc = wps_xml.describe_process_response(i, i.title(), '')

            try:
                proc = models.Process.objects.get(identifier=i)
            except models.Process.DoesNotExist:
                proc = models.Process(identifier=i, backend='local', description=desc.xml())

                proc.save()

                proc.server_set.add(server)
            else:
                proc.description = desc.xml()

                proc.save()

        logger.info('Starting CDAS2 instance monitors')

        instances = models.Instance.objects.all()

        for i in instances:
            tasks.monitor_cdas.delay(i.id)

        logger.info('Registering CDAS2 processes')

        servers = models.Server.objects.all()

        # Queue capabilities tasks for uninitialized servers
        celery.group(tasks.capabilities.s(x.id)
                     for x in servers if x.capabilities == '')()
