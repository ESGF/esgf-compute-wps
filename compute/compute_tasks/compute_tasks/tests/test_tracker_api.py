import datetime
import os
import time

import cwt
import pytest

from compute_tasks.context.tracker_api import TrackerAPI
from compute_tasks import context
from compute_tasks import utilities
from compute_tasks import WPSError


def test_build_output_variable(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'build_output')

    state.build_output_variable('tas', name='test_output')

    state.build_output.assert_called_with('nc', 'application/netcdf', var_name='tas', name='test_output')


def test_build_output(mocker):
    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/tmp',
    })

    state = TrackerAPI()
    state.user = 0
    state.job = 0

    mocker.patch.object(state, 'track_output')

    path = state.build_output('nc', 'application/netcdf', filename='test', var_name='tas', name='test_output')

    state.track_output.assert_called_with('/tmp/0/0/test.nc')

    variable = state.output[0]

    assert variable.uri == '/tmp/0/0/test.nc'
    assert variable.var_name == 'tas'
    assert variable.name == 'test_output'
    assert variable.mime_type == 'application/netcdf'

    assert path == '/tmp/0/0/test.nc'


def test_generate_local_path(mocker):
    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/tmp',
    })

    state = TrackerAPI()
    state.user = 0
    state.job = 0

    mocker.patch.object(context.tracker, 'uuid')

    context.tracker.uuid.uuid4.return_value = '1234'

    mocker.patch.object(context.tracker.os.path, 'exists')
    mocker.patch.object(context.tracker.os, 'makedirs')

    context.tracker.os.path.exists.return_value = False

    path = state.generate_local_path('nc')

    assert path == '/tmp/0/0/1234.nc'

    context.tracker.os.path.exists.assert_called_with('/tmp/0/0')
    context.tracker.os.makedirs.assert_called_with('/tmp/0/0')


def test_track_output(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    state.track_output('https://127.0.0.1/test.nc')

    params = {
        'id': None,
        'path': 'https://127.0.0.1/test.nc',
    }

    state.action.assert_called_with(['jobs', 'set_output'], params, validate=False)


def test_details(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    state.user_details()

    params = {
        'id': None
    }

    state.action.assert_called_with(['user', 'details'], params)


def test_user_cert(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    output = state.user_cert()

    params = {
        'id': None
    }

    state.action.assert_called_with(['user', 'certificate'], params)

    state.action.return_value.__getitem__.assert_called_with('certificate')

    assert output == state.action.return_value.__getitem__.return_value


def test_files_distinct_users(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    state.files_distinct_users()

    state.action.assert_called_with(['files', 'distinct_users'])


def test_unique_set(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    state.unique_status()

    state.action.assert_called_with(['status', 'unique_count'])


def test_track_process(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    state.track_process()

    params = {
        'user_pk': None,
        'process_pk': None,
    }

    state.action.assert_called_with(['user', 'process', 'create'], params)


def test_track_file(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    state.track_file(cwt.Variable('file:///test.nc', 'tas'))

    params = {
        'user_pk': None,
        'url': 'file:///test.nc',
        'var_name': 'tas',
    }

    state.action.assert_called_with(['user', 'file', 'create'], params)


def test_register_process_unique(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    state.action.side_effect = context.tracker_api.coreapi.exceptions.ErrorMessage('unique set')

    with pytest.raises(context.tracker_api.ProcessExistsError):
        state.register_process(identifier='CDAT.subset')

    ignore = (context.tracker_api.coreapi.exceptions.ErrorMessage, )

    state.action.assert_called_with(['process', 'create'], {'identifier': 'CDAT.subset'}, raise_errors=ignore)


def test_register_process_exception(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    state.action.side_effect = context.tracker_api.coreapi.exceptions.ErrorMessage('hello')

    with pytest.raises(context.tracker_api.coreapi.exceptions.ErrorMessage):
        state.register_process(identifier='CDAT.subset')

    ignore = (context.tracker_api.coreapi.exceptions.ErrorMessage, )

    state.action.assert_called_with(['process', 'create'], {'identifier': 'CDAT.subset'}, raise_errors=ignore)


def test_register_process(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    state.register_process(identifier='CDAT.subset')

    ignore = (context.tracker_api.coreapi.exceptions.ErrorMessage, )

    state.action.assert_called_with(['process', 'create'], {'identifier': 'CDAT.subset'}, raise_errors=ignore)


def test_processes(mocker):
    state = TrackerAPI()

    mocker.patch.object(state, 'action')

    output = state.processes()

    state.action.assert_called_with(['process', 'list'])

    assert output == state.action.return_value.__getitem__.return_value


def test_succeeded(mocker):
    state = TrackerAPI()
    state.job = 10
    state.status = 22

    mocker.patch.object(state, 'action')

    state.succeeded('Output')

    params = {
        'job_pk': 10,
        'status': 'ProcessSucceeded',
        'output': 'Output',
    }

    state.action.assert_called_with(['jobs', 'status', 'create'], params)


def test_failed(mocker):
    state = TrackerAPI()
    state.job = 10
    state.status = 22

    mocker.patch.object(state, 'action')

    e = Exception('Error')

    state.failed(e)

    params = {
        'job_pk': 10,
        'status': 'ProcessFailed',
        'exception': e,
    }

    state.action.assert_called_with(['jobs', 'status', 'create'], params)


def test_started(mocker):
    state = TrackerAPI()
    state.job = 10
    state.status = 22

    mocker.patch.object(state, 'action')

    state.started()

    params = {
        'job_pk': 10,
        'status': 'ProcessStarted',
    }

    state.action.assert_called_with(['jobs', 'status', 'create'], params)


def test_accepted(mocker):
    state = TrackerAPI()
    state.job = 10
    state.status = 22

    mocker.patch.object(state, 'action')

    state.accepted()

    params = {
        'job_pk': 10,
        'status': 'ProcessAccepted',
    }

    state.action.assert_called_with(['jobs', 'status', 'create'], params)


def test_message(mocker):
    state = TrackerAPI()
    state.job = 10
    state.status = 22

    mocker.patch.object(state, 'action')

    state.message('Test: {!s}', 'hello', percent=10.0)

    params = {
        'job_pk': 10,
        'status_pk': 22,
        'message': 'Test: hello',
        'percent': 10.0,
    }

    state.action.assert_called_with(['jobs', 'status', 'message', 'create'], params)


def test_set_status_exception(mocker):
    state = TrackerAPI()
    state.job = 10
    state.status = 22

    mocker.patch.object(state, 'action')

    state.action.side_effect = WPSError

    with pytest.raises(WPSError):
        state.set_status('Test', output='Output', exception='Error')

    params = {
        'job_pk': 10,
        'status': 'Test',
        'output': 'Output',
        'exception': 'Error'
    }

    state.action.assert_called_with(['jobs', 'status', 'create'], params)


def test_set_status(mocker):
    state = TrackerAPI()
    state.job = 10
    state.status = 22

    mocker.patch.object(state, 'action')

    state.set_status('Test', output='Output', exception='Error')

    params = {
        'job_pk': 10,
        'status': 'Test',
        'output': 'Output',
        'exception': 'Error'
    }

    state.action.assert_called_with(['jobs', 'status', 'create'], params)


def test_action_ignore_exception(mocker):
    mocker.patch.dict(os.environ, {
        'INTERNAL_LB': '127.0.0.1',
        'API_USERNAME': 'wps_api_user',
        'API_PASSWORD': 'wps_api_password',
    })
    coreapi = mocker.patch('compute_tasks.context.tracker_api.coreapi')
    mocker.patch.object(utilities, 'retry')

    state = TrackerAPI()

    mocker.spy(state, 'init_api')

    utilities.retry.return_value.return_value.side_effect = Exception()

    with pytest.raises(Exception):
        state.action('key1', raise_errors=(Exception, ))

    state.init_api.assert_called()

    utilities.retry.assert_called_with(count=4, delay=4, raise_errors=(Exception, ))


def test_action_exception(mocker):
    mocker.patch.dict(os.environ, {
        'INTERNAL_LB': '127.0.0.1',
        'API_USERNAME': 'wps_api_user',
        'API_PASSWORD': 'wps_api_password',
    })
    coreapi = mocker.patch('compute_tasks.context.tracker_api.coreapi')
    mocker.patch.object(utilities, 'retry')

    state = TrackerAPI()

    mocker.spy(state, 'init_api')

    utilities.retry.return_value.return_value.side_effect = Exception()

    with pytest.raises(WPSError):
        state.action('key1')

    state.init_api.assert_called()

    utilities.retry.assert_called_with(count=4, delay=4, raise_errors=())


def test_action(mocker):
    mocker.patch.dict(os.environ, {
        'INTERNAL_LB': '127.0.0.1',
        'API_USERNAME': 'wps_api_user',
        'API_PASSWORD': 'wps_api_password',
    })
    coreapi = mocker.patch('compute_tasks.context.tracker_api.coreapi')
    mocker.patch.object(utilities, 'retry')

    state = TrackerAPI()

    mocker.spy(state, 'init_api')

    state.action('key1')

    state.init_api.assert_called()

    utilities.retry.assert_called_with(count=4, delay=4, raise_errors=())


def test_update_metrics_domain_dict(mocker):
    now = datetime.datetime.now()

    state = TrackerAPI()
    state.domain = cwt.Domain(time=slice(10, 20, 2), name='d0')
    state.metrics = {
        'process_start': now,
        'process_stop': now,
        'bytes_src': 1000,
        'bytes_in': 2000,
        'bytes_out': 3000,
    }
    state.inputs = [
        cwt.Variable('http://127.0.0.1/test.nc', 'tas'),
    ]
    state.operation = cwt.Process(identifier='CDAT.subset')

    mocker.patch.object(state, 'action')
    mocker.patch.object(state, 'track_process')
    mocker.patch.object(state, 'track_file')

    state.update_metrics('ProcessStarted')

def test_update_metrics(mocker):
    now = datetime.datetime.now()

    state = TrackerAPI()
    state.domain = {
        'd0': cwt.Domain(time=slice(10, 20, 2)),
    }
    state.metrics = {
        'process_start': now,
        'process_stop': now,
        'bytes_src': 1000,
        'bytes_in': 2000,
        'bytes_out': 3000,
    }
    state.inputs = [
        cwt.Variable('http://127.0.0.1/test.nc', 'tas'),
    ]
    state.operation = cwt.Process(identifier='CDAT.subset')

    mocker.patch.object(state, 'action')
    mocker.patch.object(state, 'track_process')
    mocker.patch.object(state, 'track_file')

    state.update_metrics('ProcessStarted')

    state.track_process.assert_called()

def test_metrics():
    state = TrackerAPI()

    state.track_src_bytes(1000)

    state.track_in_bytes(2000)

    state.track_out_bytes(3000)

    assert state.metrics['bytes_src'] == 1000
    assert state.metrics['bytes_in'] == 2000
    assert state.metrics['bytes_out'] == 3000


def test_store_state():
    state = TrackerAPI()

    state.extra = {'DASK_SCHEDULER': '127.0.0.1'}
    state.job = 0
    state.user = 0
    state.process = 0
    state.status = 'ProcessStarted'
    state.metrics = {}
    state.output = []

    stored = state.store_state()

    assert set(stored.keys()) <= set(['extra', 'job', 'user', 'process', 'status', 'metrics', 'output'])


def test_context_init_api(mocker):
    requests = mocker.patch('compute_tasks.context.tracker_api.requests')
    coreapi = mocker.patch('compute_tasks.context.tracker_api.coreapi')
    mocker.patch.dict(os.environ, {
        'INTERNAL_LB': '127.0.0.1',
        'API_USERNAME': 'wps_api_user',
        'API_PASSWORD': 'wps_api_password',
    })

    state = TrackerAPI()

    state.init_api()

    client = coreapi.Client

    basic = coreapi.auth.BasicAuthentication

    basic.assert_called_with('wps_api_user', 'wps_api_password')

    transport = coreapi.transports.HTTPTransport

    transport.assert_called_with(auth=basic.return_value, session=requests.Session.return_value)

    client.assert_called_with(transports=[transport.return_value, ])

    client.return_value.get.assert_called_with('https://127.0.0.1/internal_api/schema')


def test_context_init_state(mocker):
    state = TrackerAPI()

    data = {
        'extra': {'DASK_SCHEDULER': '127.0.0.1'},
        'job': 0,
        'user': 0,
        'process': 0,
        'status': 'ProcessStarted',
        'metrics': {},
    }

    state.init_state(data)

    assert state.extra == {'DASK_SCHEDULER': '127.0.0.1'}
    assert state.job == 0
    assert state.user == 0
    assert state.process == 0
    assert state.status == 'ProcessStarted'
    assert state.metrics == {}


def test_process_timer(mocker):
    c = mocker.MagicMock()

    with context.ProcessTimer(c):
        time.sleep(1)

    c.metrics.__setitem__.assert_called()
    assert c.metrics.__setitem__.call_count == 2


def test_retry_error_raised(mocker):
    mocker.patch.object(utilities.time, 'sleep')

    class Test(object):
        def __init__(self):
            self.cnt = 0

        @utilities.retry(4, 1, raise_errors=(Exception, ))
        def test(self):
            if self.cnt <= 2:
                self.cnt += 1

                raise Exception('error')

            return None

    t = Test()

    with pytest.raises(Exception):
        t.test()


def test_retry_failure(mocker):
    mocker.patch.object(utilities.time, 'sleep')

    class Test(object):
        def __init__(self):
            self.cnt = 0

        @utilities.retry(4, 1)
        def test(self):
            raise Exception('error')

    t = Test()

    mocker.spy(t, 'test')

    with pytest.raises(Exception):
        t.test()

    assert t.test.call_count == 1
    utilities.time.sleep.assert_any_call(1)
    utilities.time.sleep.assert_any_call(2)
    utilities.time.sleep.assert_any_call(4)


def test_retry_error(mocker):
    mocker.patch.object(utilities.time, 'sleep')

    class Test(object):
        def __init__(self):
            self.cnt = 0

        @utilities.retry(4, 1)
        def test(self):
            if self.cnt <= 2:
                self.cnt += 1

                raise Exception('error')

            return None

    t = Test()

    mocker.spy(t, 'test')

    t.test()

    assert t.test.call_count == 1
    utilities.time.sleep.assert_any_call(1)
    utilities.time.sleep.assert_any_call(2)
    utilities.time.sleep.assert_any_call(4)


def test_retry(mocker):
    class Test(object):
        @utilities.retry(4, 1)
        def test(self):
            return None

    t = Test()

    mocker.spy(t, 'test')

    t.test()

    assert t.test.call_count == 1
