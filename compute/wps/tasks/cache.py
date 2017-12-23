import os

from celery import current_app
from celery import shared_task
from celery.utils.log import get_task_logger
from django.db.models import Sum
from django.utils import timezone

from wps import models
from wps import settings

logger = get_task_logger('wps.tasks.cache')

@current_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(settings.CACHE_CHECK, cache_clean.s())

@shared_task
def cache_clean():
    cached = models.Cache.objects.all()

    logger.info('Current cache consists of "{}" entries'.format(len(cached)))

    # Might need to consider what to do if the files are on shared filesystem
    # e.g. GPFS that can go down and make it look like the files have been
    # removed TODO implement counter for times missing
    for item in cached:
        if not os.path.exists(item.local_path):
            logger.info('Removing cache file "{}" which no longer exists on disk'.format(item.local_path))

            item.delete()

    # Look for expired/stale cache entries
    threshold = timezone.now() - settings.CACHE_MAX_AGE

    cached = models.Cache.objects.filter(accessed_date__lt=threshold)

    logger.info('Found {} cache entries older than {}, that have expired'.format(len(cached), threshold))

    for item in cached:
        logger.info('Removing cache file "{}"'.format(item.local_path))

        os.remove(item.local_path)

        item.delete()

    used_space = models.Cache.objects.all().aggregate(Sum('size'))['size__sum']

    if used_space is not None and used_space >= settings.CACHE_GB_MAX_SIZE:
        freed_space = 0
        to_remove = []

        logger.info('Free additional space "{}" GB used of "{}" GB'.format(used_space, settings.CACHE_GB_MAX_SIZE))

        target_used_space = float(used_space) * (1.0-settings.CACHE_FREED_PERCENT)

        logger.info('Target used space "{}" GB'.format(target_used_space))

        cache = models.Cache.objects.order_by('-accessed_date')

        for entry in cache:
            logger.info('Candidate "{}" to free "{}" GB'.format(entry.local_path, entry.size))

            freed_space = freed_space + entry.size

            to_remove.append(entry)

            if (used_space - freed_space) < target_used_space:
                break

        for entry in to_remove:
            logger.info('Removing "{}"'.format(entry.local_path))

            if os.path.exists(entry.local_path):
                os.remove(entry.local_path)

            entry.delete()
