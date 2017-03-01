#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

from celery.utils.log import get_task_logger
from celery import shared_task

logger = get_task_logger(__name__)
