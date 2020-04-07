import json

import cwt
from celery.utils.log import get_task_logger

from compute_tasks import base
from compute_tasks import mapper
from compute_tasks import WPSError
from compute_tasks.context.base_context import BaseContext
from compute_tasks.context.tracker_api import TrackerAPI

logger = get_task_logger(__name__)

class OperationContext(TrackerAPI, BaseContext):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
