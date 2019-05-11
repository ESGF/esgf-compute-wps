import os
import logging

from wps import models

from django.db.models.signals import post_delete
from django.dispatch import receiver

logger = logging.getLogger('wps.signals')


@receiver(post_delete, sender=models.Output)
def delete_output_handler(sender, instance, **kwargs):
    logger.info('Removing %r', instance.path)

    try:
        os.remove(instance.path)
    except OSError:
        logger.debug('Error removing file %r', instance.path)

        pass
