import os
import time
import uuid
import urllib
from datetime import datetime

import cwt
import coreapi
import requests
from celery.utils.log import get_task_logger

from compute_tasks import metrics_ as metrics
from compute_tasks import WPSError

logger = get_task_logger('wps.context')

SUCCESS = 'success'
FAILURE = 'failure'

INTERNAL_LB = os.environ.get('INTERNAL_LB', '')
API_USERNAME = os.environ.get('API_USERNAME', '')
API_PASSWORD = os.environ.get('API_PASSWORD', '')
DATA_PATH = os.environ.get('DATA_PATH', '')


def retry(count, delay, ignore):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            last_exc = None
            completed = False

            for x in range(count):
                try:
                    data = func(*args, **kwargs)
                except Exception as e:
                    if isinstance(e, ignore):
                        raise e

                    last_exc = e
                else:
                    completed = True

                    break

                time.sleep(5)

                logger.info('Retrying...')

            if not completed:
                raise last_exc

            return data
        return wrapped
    return wrapper


class ProcessExistsError(WPSError):
    pass


class ProcessTimer(object):
    def __init__(self, context):
        self.context = context

    def __enter__(self):
        self.context.metrics['process_start'] = datetime.now()

    def __exit__(self, *args):
        self.context.metrics['process_stop'] = datetime.now()


class StateMixin(object):
    def __init__(self):
        self.job = None
        self.user = None
        self.process = None
        self.status = None
        self.client = None
        self.schema = None
        self.metrics = {}
        self.output = []

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
        # Covers the case where domain is None
        domain = self.domain or {}

        if not isinstance(domain, dict):
            domain = {domain.name: domain}

        for item in domain.values():
            for name, value in item.dimensions.items():
                metrics.WPS_DOMAIN_CRS.labels(name, str(value.crs)).inc()

        self.track_process()

        if 'process_start' in self.metrics and 'process_stop' in self.metrics:
            elapsed = (self.metrics['process_stop'] - self.metrics['process_start']).total_seconds()

            if isinstance(self, OperationContext):
                identifier = self.operation.identifier
            else:
                identifier = 'CDAT.workflow'

            metrics.WPS_PROCESS_TIME.labels(identifier, state).observe(elapsed)

        if set(['bytes_src', 'bytes_in', 'bytes_out']) <= set(self.metrics.keys()):
            metrics.WPS_DATA_SRC_BYTES.inc(self.metrics['bytes_src'])

            metrics.WPS_DATA_IN_BYTES.inc(self.metrics['bytes_in'])

            metrics.WPS_DATA_OUT_BYTES.inc(self.metrics['bytes_out'])

        for input in self.inputs:
            self.track_file(input)

            parts = urllib.parse.urlparse(input.uri)

            metrics.WPS_FILE_ACCESSED.labels(parts.hostname, input.var_name).inc()

    def init_state(self, data):
        self.job = data['job']

        self.user = data['user']

        self.process = data['process']

        self.status = data.get('status')

        self.metrics = data.get('metrics', {})

        logger.info('Initial with %r', data)

        self.init_api()

    def init_api(self):
        session = requests.Session()

        session.verify = False

        auth = coreapi.auth.BasicAuthentication(API_USERNAME, API_PASSWORD)

        transport = coreapi.transports.HTTPTransport(auth=auth, session=session)

        self.client = coreapi.Client(transports=[transport, ])

        schema_url = 'https://{!s}/internal_api/schema'.format(INTERNAL_LB)

        self.schema = self.client.get(schema_url)

    def store_state(self):
        return {
            'job': self.job,
            'user': self.user,
            'process': self.process,
            'status': self.status,
            'metrics': self.metrics,
            'output': self.output,
        }

    def action(self, keys, params=None, **kwargs):
        ignore_errors = kwargs.pop('ignore_errors', ())

        r = retry(count=4, delay=4, ignore=ignore_errors)(self.client.action)

        try:
            return r(self.schema, keys, params=params, **kwargs)
            # return self.client.action(self.schema, keys, params=params, **kwargs)
        except Exception as e:
            if isinstance(e, ignore_errors):
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

        logger.info('%s', msg)

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
            self.action(['process', 'create'], params, ignore_errors=(coreapi.exceptions.ErrorMessage, ))
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

    def build_output_variable(self, var_name, name=None):
        return self.build_output('nc', 'application/netcdf', var_name=var_name, name=name)

    def build_output(self, extension, mime_type, filename=None, var_name=None, name=None):
        local_path = self.generate_local_path(extension)

        self.track_output(local_path)

        self.output.append(cwt.Variable(local_path, var_name, name=name, mime_type=mime_type))

        return local_path

    def generate_local_path(self, extension, filename=None):
        if filename is None:
            filename = str(uuid.uuid4())

        filename_ext = '{!s}.{!s}'.format(filename, extension)

        base_path = os.path.join(DATA_PATH, str(self.user), str(self.job))

        if not os.path.exists(base_path):
            os.makedirs(base_path)

        return os.path.join(base_path, filename_ext)


class WorkflowOperationContext(StateMixin, object):
    def __init__(self, inputs, domain, operation):
        super(WorkflowOperationContext, self).__init__()

        self._inputs = inputs
        self.domain = domain
        self.operation = operation

    @classmethod
    def from_data_inputs(cls, variable, domain, operation):
        for op in operation.values():
            inputs = []

            for x in op.inputs:
                if x in variable:
                    inputs.append(variable[x])
                elif x in operation:
                    inputs.append(operation[x])
                else:
                    raise WPSError('Unable to find input {!r}', x)

            op.inputs = inputs

            op.domain = domain.get(op.domain, None)

        instance = cls(variable, domain, operation)

        return instance

    @classmethod
    def from_dict(cls, data):
        variable = data.pop('_inputs')

        domain = data.pop('domain')

        operation = data.pop('operation')

        for x in operation.values():
            inputs = []

            for y in x.inputs:
                if y in variable:
                    inputs.append(variable[y])
                elif y in operation:
                    inputs.append(operation[y])
                else:
                    raise WPSError('Failed to resolve input {!r}', y)

            x.inputs = inputs

            try:
                x.domain = domain[x.domain]
            except KeyError:
                pass

        instance = cls(variable, domain, operation)

        instance.output = data.pop('output')

        instance.init_state(data)

        return instance

    def to_dict(self):
        data = {
            '_inputs': self._inputs,
            'domain': self.domain,
            'operation': self.operation,
        }

        data.update(self.store_state())

        return data

    @property
    def inputs(self):
        return self._inputs.values()

    def output_ops(self):
        out_deg = dict((x, self.node_out_deg(x)) for x in self.operation.keys())

        return [self.operation[x] for x, y in out_deg.items() if y == 0]

    def node_in_deg(self, node):
        return len([x for x in node.inputs if x.name in self.operation])

    def node_out_deg(self, node):
        return len([x for x, y in self.operation.items() if any(node == z.name for z in y.inputs)])

    def find_neighbors(self, node):
        return [x for x, y in self.operation.items() if node in [x.name for x in y.inputs]]

    def topo_sort(self):
        in_deg = dict((x, self.node_in_deg(y)) for x, y in self.operation.items())

        neigh = dict((x, self.find_neighbors(x)) for x in in_deg.keys())

        queue = [x for x, y in in_deg.items() if y == 0]

        cnt = 0

        topo_order = []

        while queue:
            next = queue.pop(0)

            topo_order.append(self.operation[next])

            for x in neigh[next]:
                in_deg[x] -= 1

                if in_deg[x] == 0:
                    queue.append(x)

            cnt += 1

        if cnt != len(self.operation):
            raise WPSError('Failed to compute the graph')

        logger.info('Result of topo sort %r', topo_order)

        return topo_order


class OperationContext(StateMixin, object):
    def __init__(self, inputs=None, domain=None, operation=None):
        super(OperationContext, self).__init__()

        self.inputs = inputs
        self.domain = domain
        self.operation = operation

    @classmethod
    def from_data_inputs(cls, identifier, variable, domain, operation):
        try:
            target_op = [x for x in list(operation.values()) if x.identifier ==
                         identifier][0]
        except IndexError:
            raise WPSError('Unable to find operation {!r}', identifier)

        logger.info('Target operation %r', target_op)

        target_domain = domain.get(target_op.domain, None)

        logger.info('Target domain %r', target_domain)

        try:
            target_inputs = [variable[x] for x in target_op.inputs]
        except KeyError as e:
            raise WPSError('Missing variable with name {!r}', str(e))

        logger.info('Target inputs %r', target_inputs)

        return cls(target_inputs, target_domain, target_op)

    @classmethod
    def from_dict(cls, data):
        try:
            inputs = data.pop('inputs')

            domain = data.pop('domain')

            operation = data.pop('operation')
        except KeyError:
            raise WPSError('Unabled to load operation context')
        else:
            obj = cls(inputs, domain, operation)

        obj.output = data.pop('output')

        obj.init_state(data)

        return obj

    def to_dict(self):
        data = {
            'inputs': self.inputs,
            'domain': self.domain,
            'operation': self.operation,
        }

        data.update(self.store_state())

        return data

    @property
    def is_regrid(self):
        return 'gridder' in self.operation.parameters
