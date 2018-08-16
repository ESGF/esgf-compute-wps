#! /usr/bin/env python

import os
from celery.utils.log import get_task_logger

from django.db.models.signals import post_save
from django.db.models.signals import post_delete
from django.dispatch import receiver

from wps import metrics
from wps import models

logger = get_task_logger('wps.signals')

@receiver(post_save, sender=models.Cache)
def cache_save(sender, instance, **kwargs):
    entries = model.Cache.objects.all()

    metrics.CACHE_BYTES.observe(sum(x.size for x in entries))

    metrics.CACHE_FILES.observe(entries.count())

@receiver(post_delete, sender=models.Cache)
def cache_delete(sender, instance, **kwargs):
    entries = model.Cache.objects.all()

    metrics.CACHE_BYTES.observe(sum(x.size for x in entries))

    metrics.CACHE_FILES.observe(entries.count())

    if os.path.exists(instance.local_path):
        os.remove(instance.local_path)

        logger.info('Removed cached file %r', instance.local_path)
