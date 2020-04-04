from datetime import datetime

from .tracker_api import TrackerAPI
from .operation import OperationContext

SUCCESS = 'success'
FAILURE = 'failure'


class ProcessTimer(object):
    def __init__(self, context):
        self.context = context

    def __enter__(self):
        self.context.metrics['process_start'] = datetime.now()

    def __exit__(self, *args):
        self.context.metrics['process_stop'] = datetime.now()
