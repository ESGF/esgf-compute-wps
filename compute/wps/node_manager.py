#! /usr/bin/env python

import logging
import time
import os

import django
import redis

from wps import models
from wps import tasks

logger = logging.getLogger(__name__)

class NodeManager(object):

    def connect_redis(self):
        host = os.getenv('REDIS_HOST', '0.0.0.0')

        port = os.getenv('REDIS_PORT', 6379)

        db = os.getenv('REDIS_DB', 0)

        return redis.Redis(host, port, db)

    def initialize(self):
        wps_init = os.getenv('WPS_NO_INIT')

        if wps_init is not None:
            return

        logger.info('Initializing node manager')

        try:
            instances = models.Instance.objects.all()
        except django.db.utils.ProgrammingError:
            logger.info('Database does not appear to be setup yet, skipping initialization')

            return

        if len(instances) > 0:
            redis = self.connect_redis()

            init = redis.get('init')

            if init is None:
                tasks.instance_capabilities.delay(instances[0].id)

                while redis.get('init') is None:
                    time.sleep(1)

                logger.info('Initialization done')
            
            time.sleep(10)

            redis.delete('init')
        else:
            logger.info('No CDAS instances were found to querying capabilities')
