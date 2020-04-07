import hashlib
import os
import uuid
import urllib
import time

import cwt
import coreapi
import requests
from celery.utils.log import get_task_logger

from compute_tasks import metrics_ as metrics
from compute_tasks import utilities
from compute_tasks import WPSError
from compute_tasks.context import tracker

logger = get_task_logger(__name__)


class ProcessExistsError(WPSError):
    pass


class TrackerAPI(tracker.Tracker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.status = None
        self.client = None
        self.schema = None
        self.output = []

    def init_state(self, data):
        self.extra = data['extra']

        self.job = data['job']

        self.user = data['user']

        self.process = data['process']

        self.status = data.get('status')

        self.metrics = data.get('metrics', {})

    def store_state(self):
        return {
            'extra': self.extra,
            'job': self.job,
            'user': self.user,
            'process': self.process,
            'status': self.status,
            'metrics': self.metrics,
            'output': self.output,
        }

    def init_api(self):
        session = requests.Session()

        session.verify = False

        API_USERNAME = os.environ['API_USERNAME']

        API_PASSWORD = os.environ['API_PASSWORD']

        auth = coreapi.auth.BasicAuthentication(API_USERNAME, API_PASSWORD)

        transport = coreapi.transports.HTTPTransport(auth=auth, session=session)

        self.client = coreapi.Client(transports=[transport, ])

        schema_url = 'https://{!s}/internal_api/schema'.format(os.environ.get('INTERNAL_LB', '127.0.0.1'))

        self.schema = self.client.get(schema_url)

    def track_src_bytes(self, nbytes):
        self._track_bytes('bytes_src', nbytes)

    def track_in_bytes(self, nbytes):
        self._track_bytes('bytes_in', nbytes)

    def track_out_bytes(self, nbytes):
        self._track_bytes('bytes_out', nbytes)

    def _track_bytes(self, key, nbytes):
        if key not in self.metrics:
            self.metrics[key] = 0

        self.metrics[key] += nbytes

    def update_metrics(self, state):
        self.track_process()

        if set(['bytes_src', 'bytes_in', 'bytes_out']) <= set(self.metrics.keys()):
            metrics.WPS_DATA_SRC_BYTES.inc(self.metrics['bytes_src'])

            metrics.WPS_DATA_IN_BYTES.inc(self.metrics['bytes_in'])

            metrics.WPS_DATA_OUT_BYTES.inc(self.metrics['bytes_out'])

    def action(self, keys, params=None, **kwargs):
        if self.client is None:
            self.init_api()

        raise_errors = kwargs.pop('raise_errors', ())

        r = utilities.retry(count=4, delay=4, raise_errors=raise_errors)(self.client.action)

        logger.debug('Action keys %r params %r kwargs %r', keys, params, kwargs)

        try:
            return r(self.schema, keys, params=params, **kwargs)
        except Exception as e:
            if isinstance(e, raise_errors):
                raise e

            logger.debug('Params %r kwargs %r', params, kwargs)

            raise WPSError('Internal API call failed {!r}', e)

    def set_status(self, status, output=None, exception=None):
        params = {
            'job_pk': self.job,
            'status': status,
        }

        if output is not None:
            params['output'] = output

        if exception is not None:
            params['exception'] = exception

        try:
            output = self.action(['jobs', 'status', 'create'], params)
        except WPSError:
            raise
        else:
            self.status = output['id']

    def message(self, fmt, *args, **kwargs):
        percent = kwargs.get('percent', 0.0)

        msg = fmt.format(*args, **kwargs)

        logger.info('%s: %r', msg, percent)

        params = {
            'job_pk': self.job,
            'status_pk': self.status,
            'message': msg,
            'percent': percent,
        }

        self.action(['jobs', 'status', 'message', 'create'], params)

    def accepted(self):
        self.set_status('ProcessAccepted')

    def started(self):
        self.set_status('ProcessStarted')

    def failed(self, exception):
        self.set_status('ProcessFailed', exception=exception)

    def succeeded(self, output):
        self.set_status('ProcessSucceeded', output=output)

    def processes(self):
        output = self.action(['process', 'list'])

        return output['results']

    def register_process(self, **params):
        try:
            self.action(['process', 'create'], params, raise_errors=(coreapi.exceptions.ErrorMessage, ))
        except coreapi.exceptions.ErrorMessage as e:
            if 'unique set' in str(e):
                raise ProcessExistsError()

            logger.exception('Failed with %r', params)

            raise e

    def track_file(self, file):
        params = {
            'user_pk': self.user,
            'url': file.uri,
            'var_name': file.var_name,
        }

        self.action(['user', 'file', 'create'], params)

    def track_process(self):
        params = {
            'user_pk': self.user,
            'process_pk': self.process,
        }

        self.action(['user', 'process', 'create'], params)

    def unique_status(self):
        return self.action(['status', 'unique_count'])

    def files_distinct_users(self):
        return self.action(['files', 'distinct_users'])

    def user_cert(self):
        params = {
            'id': self.user,
        }

        return self.action(['user', 'certificate'], params)['certificate']

    def user_details(self):
        params = {
            'id': self.user,
        }

        return self.action(['user', 'details'], params)

    def track_output(self, path):
        params = {
            'id': self.job,
            'path': path,
        }

        return self.action(['jobs', 'set_output'], params, validate=False)
