#! /usr/bin/env python

import os
from celery.utils.log import get_task_logger

from django.conf import settings
from django.contrib.auth.signals import user_logged_in
from django.contrib.auth.signals import user_logged_out
from django.db.models.signals import post_save
from django.db.models.signals import post_delete
from django.dispatch import receiver
from django.utils import timezone

from wps import metrics
from wps import models

logger = get_task_logger('wps.signals')

def is_active(user):
    try:
        last = timezone.now() - user.last_login
    except TypeError:
        # Handle when user.last_login is None
        return False

    return last <= settings.ACTIVE_USER_THRESHOLD

@receiver(user_logged_in)
def user_login(sender, request, user, **kwargs):
    active_users = [x for x in models.User.objects.all() if is_active(x)]

    metrics.USERS_ACTIVE.set(len(active_users))

@receiver(user_logged_out)
def user_logout(sender, request, user, **kwargs):
    active_users = [x for x in models.User.objects.all() if is_active(x)]

    metrics.USERS_ACTIVE.set(len(active_users))

@receiver(post_save, sender=models.Cache)
def cache_save(sender, instance, **kwargs):
    logger.info('Increasing cache by %r', instance.size)

    metrics.CACHE_BYTES.inc(float(instance.size))

    metrics.CACHE_FILES.inc()

@receiver(post_delete, sender=models.Cache)
def cache_delete(sender, instance, **kwargs):
    metrics.CACHE_BYTES.dec(float(instance.size))

    metrics.CACHE_FILES.dec()

    if os.path.exists(instance.local_path):
        os.remove(instance.local_path)

        logger.info('Removed cached file %r', instance.local_path)
