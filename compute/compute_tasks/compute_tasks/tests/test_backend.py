import os
from argparse import Namespace

from compute_tasks import backend
from compute_tasks import base
from compute_tasks.context import state_mixin

RAW_FRAMES_ERROR = [
    b'2.2.0',
    b'CDAT.subset',
    b'{"variable": [], "domain": [], "operation": []}',
    b'0',
    b'0',
    b'0',
    b'RESOURCES',
    b'Error',
]

RAW_FRAMES = [
    b'2.2.0',
    b'CDAT.subset',
    b'{"variable": [], "domain": [], "operation": []}',
    b'0',
    b'0',
    b'0',
]

DECODED_FRAMES = [
    'CDAT.subset',
    {'variable': [], 'domain': [], 'operation': []},
    '0',
    '0',
    '0',
    {'DASK_SCHEDULER': 'dask-scheduler-0'}
]


def test_main(mocker):
    mocker.patch.object(backend, 'state_mixin')
    mocker.patch.object(backend, 'Worker')
    mocker.patch.object(backend, 'load_processes')
    mocker.patch.dict(os.environ, {
        'PROVISIONER_BACKEND': '127.0.0.1',
    })
    mocker.patch.object(backend, 'backend_argparse')

    ns = Namespace(log_level='INFO', queue_host='127.0.0.1', skip_register_tasks=False, skip_init_api=False, d=False)

    backend.backend_argparse.return_value.parse_args.return_value = ns

    backend.main()

    backend.load_processes.assert_called()

    backend.state_mixin.StateMixin.return_value.init_api.assert_called()

    backend.Worker.return_value.run.assert_called()


def test_reload_processes(mocker):
    mocker.patch.object(backend, 'state_mixin')
    mocker.patch.object(backend, 'load_processes')
    mocker.patch.object(backend, 'reload_argparse')

    backend.reload_processes()

    backend.load_processes.assert_called()


def test_load_processes_exists(mocker):
    mocker.patch.object(base, 'discover_processes')
    mocker.patch.object(base, 'build_process_bindings')

    base.register_process.side_effect = state_mixin.ProcessExistsError('')

    state = mocker.MagicMock()

    backend.load_processes(state)

    base.discover_processes.assert_called()

    base.build_process_bindings.assert_called()

    assert state.register_process.call_count == 0


def test_load_processes_skip_register(mocker):
    mocker.patch.object(base, 'discover_processes')
    mocker.patch.object(base, 'build_process_bindings')

    base.discover_processes.return_value = [{'identifier': 'CDAT.subset'}]

    state = mocker.MagicMock()

    backend.load_processes(state, False)

    base.discover_processes.assert_not_called()

    base.build_process_bindings.assert_called()

    assert state.register_process.call_count == 0


def test_load_processes(mocker):
    mocker.patch.object(base, 'discover_processes')
    mocker.patch.object(base, 'build_process_bindings')

    base.discover_processes.return_value = [{'identifier': 'CDAT.subset'}]

    state = mocker.MagicMock()

    backend.load_processes(state)

    base.discover_processes.assert_called()

    base.build_process_bindings.assert_called()

    assert state.register_process.call_count == 1


def test_error_handler(mocker):
    mocker.patch.object(backend, 'fail_job')

    state = mocker.MagicMock()

    backend.error_handler(RAW_FRAMES_ERROR, state)

    backend.fail_job.assert_called_with(state, '0', 'Error')


def test_resource_request(mocker):
    mocker.patch.dict(os.environ, {
        'IMAGE': 'aims2.llnl.gov/compute-celery',
        'WORKERS': '10',
        'WPS_DEV': '1',
    })

    env = mocker.MagicMock()

    env.get_template.return_value.render.return_value = ''

    output = backend.resource_request(RAW_FRAMES, env)

    env.get_template.assert_has_calls([
            mocker.call('dask-scheduler-pod.yaml'),
            mocker.call('dask-scheduler-service.yaml'),
            mocker.call('dask-worker-deployment.yaml'),
            mocker.call().render(data_claim_name='data-pvc', dev='1', image='aims2.llnl.gov/compute-celery',
                                 image_pull_secret=None, user='0', workers='10'),
            mocker.call().render(data_claim_name='data-pvc', dev='1', image='aims2.llnl.gov/compute-celery',
                                 image_pull_secret=None, user='0', workers='10'),
            mocker.call().render(data_claim_name='data-pvc', dev='1', image='aims2.llnl.gov/compute-celery',
                                 image_pull_secret=None, user='0', workers='10'),
    ])

    assert output == '["", "", ""]'


def test_requeust_handler_exception(mocker):
    mocker.patch.object(backend, 'fail_job')
    mocker.patch.object(backend, 'build_workflow')

    backend.build_workflow.side_effect = Exception('Error')

    state = mocker.MagicMock()

    backend.request_handler(RAW_FRAMES, state)

    backend.fail_job.assert_called_with(state, '0', backend.build_workflow.side_effect)


def test_requeust_handler(mocker):
    mocker.patch.object(backend, 'build_workflow')

    state = mocker.MagicMock()

    backend.request_handler(RAW_FRAMES, state)

    backend.build_workflow.assert_called()

    backend.build_workflow.return_value.delay.assert_called()


def test_build_workflow(mocker):
    mocker.patch.object(base, 'get_process')

    job_started = mocker.patch.object(backend, 'job_started')

    job_succeeded = mocker.patch.object(backend, 'job_succeeded')

    workflow = backend.build_workflow(DECODED_FRAMES)

    job_started.s.assert_called_with('CDAT.subset', {'variable': [], 'domain': [], 'operation': []}, '0', '0', '0',
                                     {'DASK_SCHEDULER': 'dask-scheduler-0'})

    job_started.s.return_value.set.assert_called_with(**backend.DEFAULT_QUEUE)

    base.get_process.assert_called_with('CDAT.subset')

    base.get_process.return_value.s.assert_called()

    base.get_process.return_value.s.return_value.set.assert_called_with(**backend.DEFAULT_QUEUE)

    job_succeeded.s.assert_called()

    job_succeeded.s.return_value.set.assert_called_with(**backend.DEFAULT_QUEUE)

    assert workflow == job_started.s.return_value.set.return_value.__or__.return_value.__or__.return_value


def test_format_frames():
    output = backend.format_frames(RAW_FRAMES)

    assert len(output) == 6
    assert output[0] == 'CDAT.subset'
    assert output[1] == {'variable': [], 'domain': [], 'operation': []}
    assert output[2] == '0'
    assert output[3] == '0'
    assert output[4] == '0'
    assert output[5] == {'DASK_SCHEDULER': 'dask-scheduler-0'}


def test_fail_job_exception(mocker):
    state = mocker.MagicMock()

    state.failed.side_effect = Exception('Error')

    job = mocker.MagicMock()

    backend.fail_job(state, job, Exception('Error'))

    assert state.job is None


def test_fail_job(mocker):
    state = mocker.MagicMock()

    job = mocker.MagicMock()

    backend.fail_job(state, job, Exception('Error'))

    state.failed.assert_called_with('Error')

    assert state.job is None


def test_queue_from_identifier():
    assert backend.queue_from_identifier('CDAT.subset') == backend.QUEUE['cdat']
