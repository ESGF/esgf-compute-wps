import json
import logging
import time

import cwt
import pytest

from compute_tasks import backend
from compute_tasks import cdat
from compute_tasks import celery_ as celery

logger = logging.getLogger()

V0 = cwt.Variable('file:///test0.nc', 'tas')
V1 = cwt.Variable('file:///test1.nc', 'tas')

SUBSET = cwt.Process('CDAT.subset')
SUBSET.add_inputs(V0)
WORKFLOW = cwt.Process('CDAT.workflow')
WORKFLOW.add_inputs(SUBSET)

DATA_INPUTS = {
    'variable': [],
    'domain': [],
    'operation': [
        WORKFLOW.to_dict(),
        SUBSET.to_dict(),
    ],
}

RAW_FRAMES = [
    'CDAT.workflow',
    json.dumps(DATA_INPUTS),
    '0',
    '0',
    '0',
]

ENV = {
    'IMAGE': 'aims2.llnl.gov/compute-tasks:latest',
    'WORKERS': '8',
}


def test_worker_run(mocker, provisioner, worker):
    time.sleep(4)

    w = provisioner.workers[0]

    RAW_PROVISIONER_FRAMES = [
        backend.REQUEST,
        b'devel',
        b'CDAT.workflow',
        json.dumps(DATA_INPUTS).encode(),
        b'0',
        b'0',
        b'0',
    ]

    provisioner.send(w, RAW_PROVISIONER_FRAMES)


def test_worker_missed_heartbeat(mocker, provisioner):
    w = backend.Worker(b'devel', '127.0.0.1:8787')

    mocker.patch.object(w, 'init_api')

    w.initialize()

    w.connect_provisioner()

    w.missed_heartbeat()

    assert w.liveness == 2

    w.missed_heartbeat()

    assert w.liveness == 1

    assert w.interval == 1

    w.missed_heartbeat()

    assert w.interval == 2
    assert w.liveness == 3

    time.sleep(2)

    assert len(provisioner.received) == 2
    assert all([provisioner.received[x][1] == backend.READY for x in range(2)])


def test_worker_send_heartbeat(mocker, provisioner):
    w = backend.Worker(b'devel', '127.0.0.1:8787')

    mocker.patch.object(w, 'init_api')

    w.initialize()

    w.connect_provisioner()

    w.heartbeat_at = time.time()

    time.sleep(2)

    w.send_heartbeat()

    time.sleep(4)

    assert len(provisioner.received) == 2
    assert provisioner.received[0][1] == backend.READY
    assert provisioner.received[1][1] == backend.HEARTBEAT


def test_worker_fail_job(mocker):
    w = backend.Worker(b'devel', '127.0.0.1:8787')

    mocker.patch.object(w, 'init_api')

    mocker.spy(w, 'failed')

    w.fail_job(10, 'test')

    w.failed.assert_called_with('test')

    assert w.job is None


def test_worker_reconnect_provisioner(mocker, provisioner):
    w = backend.Worker(b'devel', '127.0.0.1:8787')

    mocker.patch.object(w, 'init_api')

    w.initialize()

    w.connect_provisioner()

    time.sleep(4)

    w.reconnect_provisioner()

    time.sleep(4)

    assert len(provisioner.received) == 2
    assert all([provisioner.received[x][1] == backend.READY for x in range(2)])
    assert all([provisioner.received[x][2] == b'devel' for x in range(2)])


def test_worker_connect_provisioner(mocker, provisioner):
    w = backend.Worker(b'devel', '127.0.0.1:8787')

    mocker.patch.object(w, 'init_api')

    w.initialize()

    w.connect_provisioner()

    time.sleep(4)

    assert len(provisioner.received) == 1
    assert provisioner.received[0][1] == backend.READY
    assert provisioner.received[0][2] == b'devel'


def test_worker_stop(worker):
    worker.stop()

    assert not worker.running


def test_worker_initialize(mocker):
    w = backend.Worker(b'devel', '127.0.0.1:8787')

    mocker.patch.object(w, 'init_api')

    w.initialize()

    assert isinstance(w.state, backend.WaitingState)


@pytest.mark.parametrize('transition,frames,expected', [
    (backend.ACK, [], backend.WaitingState),
    (backend.ERR, [b'Error message'], backend.WaitingState),
    (b'NO', [], backend.ResourceAckState),
])
def test_resource_ack_state(mocker, transition, frames, expected):
    b = mocker.MagicMock()

    mocker.patch.object(backend, 'build_workflow')

    state = backend.ResourceAckState('CDAT.subset', '{"variable": [], "domain": [], "operation": []}', '0', '0', '0')

    frames.insert(0, transition.decode())

    new_state = state.on_event(b, *frames)

    assert isinstance(new_state, expected)


@pytest.mark.parametrize('transition,patch_env,expected', [
    (backend.REQUEST, ENV, backend.ResourceAckState),
    (backend.REQUEST, {}, backend.WaitingState),
    (b'NO', {}, backend.WaitingState),
])
def test_waiting_state(mocker, transition, patch_env, expected):
    mocker.patch.dict(backend.os.environ, patch_env)

    data = {
        'image': 'aims2.llnl.gov/compute-tasks:latest',
        'image_pull_secret': None,
        'image_pull_policy': 'Always',
        'scheduler_cpu': 1,
        'scheduler_memory': '1Gi',
        'worker_cpu': 1,
        'worker_memory': '1Gi',
        'worker_nthreads': 4,
        'traffic_type': 'development',
        'dev': False,
        'user': '0',
        'workers': '8',
        'data_claim_name': 'data-pvc',
    }

    b = mocker.MagicMock()

    state = backend.WaitingState()

    new_state = state.on_event(b, transition.decode(), 'devel', 'CDAT.subset', '{"variable": [], "domain": [], "operation": []}', '0', '0', '0')

    assert isinstance(new_state, expected)


def test_build_workflow(mocker):
    mocker.patch.dict(backend.os.environ, {'NAMESPACE': 'default'})

    mocker.spy(backend, 'job_started')
    mocker.spy(backend, 'job_succeeded')

    cdat.discover_processes()

    workflow = backend.build_workflow(*RAW_FRAMES)

    assert workflow

    expected = [x for x in RAW_FRAMES]

    expected[1] = celery.decoder(expected[1])

    expected.append({
        'DASK_SCHEDULER': 'dask-scheduler-0.default.svc:8786',
    })

    backend.job_started.s.assert_called_with(*expected)
    backend.job_succeeded.s.assert_called()


@pytest.mark.parametrize('identifier,inputs,params', [
    pytest.param('CDAT.subset', [], {}, marks=pytest.mark.xfail),
    ('CDAT.subset', [V0], {}),
    pytest.param('CDAT.aggregate', [V0], {}, marks=pytest.mark.xfail),
    ('CDAT.aggregate', [V0, V1], {}),
    pytest.param('CDAT.max', [V0], {}, marks=pytest.mark.xfail),
    ('CDAT.max', [V0], {'axes': ['time']}),
    pytest.param('CDAT.add', [V0], {'const': 'asdasd'}, marks=pytest.mark.xfail),
])
def test_validate_process(identifier, inputs, params):
    process = cwt.Process(identifier)

    process.add_inputs(*inputs)

    process.add_parameters(**params)

    backend.validate_process(process)


@pytest.mark.parametrize('identifier,expected', [
    ('CDAT.subset', backend.DEFAULT_QUEUE),
])
def test_queue_from_identifier(identifier, expected):
    q = backend.queue_from_identifier(identifier)

    assert q == expected


def test_missing_heartbeat(mocker, worker):
    timeouts = []

    def reconnect_provisioner(self):
        timeouts.append(self.interval)

        if self.interval == 4:
            self.running = False

    backend.Worker.reconnect_provisioner = reconnect_provisioner

    worker.join()

    assert timeouts == [2, 4]
