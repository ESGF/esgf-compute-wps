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
        if os.getenv('WPS_NO_INIT') is not None:
            return

        # Need to import after app is ready
        from wps import models
        from wps import tasks
        from wps.processes import registry

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

        for identifier, _ in registry.iteritems():
            desc_proc = wps_xml.describe_process_response(identifier,
                                                          identifier.title(),
                                                          '')

            proc = models.Process(identifier=identifier,
                                  backend='local',
                                  description=desc_proc.xml())

            try:
                proc.save()
            except db.IntegrityError:
                logger.debug('Process entry "{}" already exists'.format(identifier))

                continue

            logger.info('Registered process "{}"'.format(identifier))

            proc.server_set.add(server)

        logger.info('Registering CDAS2 processes')

        servers = models.Server.objects.all()

        # Queue capabilities tasks for uninitialized servers
        celery.group(tasks.capabilities.s(x.id)
                for x in servers if x.capabilities == '')()
