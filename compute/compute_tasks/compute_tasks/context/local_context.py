import logging

from compute_tasks.context.base_context import BaseContext
from compute_tasks.context.tracker import Tracker

logger = logging.getLogger(__name__)

class TrackerLocal(Tracker):
    def init_state(self, data):
        pass

    def store_state(self):
        pass

    def track_src_bytes(self, nbytes):
        logger.info(f'Tracking src bytes {nbytes}')

    def track_in_bytes(self, nbytes):
        logger.info(f'Tracking in bytes {nbytes}')

    def track_out_bytes(self, nbytes):
        logger.info(f'Tracking out bytes {nbytes}')

    def update_metrics(self, state):
        logger.info(f'Update metrics {state}')

    def set_status(self, status, output=None, exception=None):
        logger.info(f'Set status {status} {output} {exception}')

    def message(self, fmt, *args, **kwargs):
        logger.info(f'{fmt.format(*args, **kwargs)} {kwargs.get("percent", 0)}')

    def accepted(self):
        logger.info('Accepted')

    def started(self):
        logger.info('Started')

    def failed(self, exception):
        logger.info('Failed')

    def succeeded(self, output):
        logger.info('Succeeded')

    def register_process(self, **params):
        logger.info(f'Register process {params}')

    def track_file(self, file):
        logger.info(f'Track file {file}')

    def track_process(self):
        logger.info(f'Track process')

    def unique_status(self):
        logger.info(f'Unique status')

    def files_distinct_users(self):
        logger.info(f'Files distinct users')

    def user_cert(self):
        logger.info(f'User cert')

    def user_details(self):
        logger.info(f'User details')

    def track_output(self, path):
        logger.info(f'Track output {path}')

class LocalContext(TrackerLocal, BaseContext):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
