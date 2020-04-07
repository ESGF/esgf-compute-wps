import os
import json
import logging
import time

import cwt
import pytest

from compute_tasks import backend
from compute_tasks import cdat
from compute_tasks import base
from compute_tasks import celery_app
from compute_tasks import WPSError
from compute_tasks.context import operation

logger = logging.getLogger()

V0 = cwt.Variable('file:///test0.nc', 'tas')
V1 = cwt.Variable('file:///test1.nc', 'tas')

SUBSET = cwt.Process(identifier='CDAT.subset')
SUBSET.add_inputs(V0)

DATA_INPUTS = {
    'variable': [V0.to_dict()],
    'domain': [],
    'operation': [SUBSET.to_dict()],
}

RAW_FRAMES = [
    'CDAT.workflow',
    json.dumps(DATA_INPUTS),
    '0',
    '0',
    '0',
]


def test_render_templates(mocker):
    mocker.patch.dict(os.environ, {
        'IMAGE': 'test',
        'REDIS_HOST': '127.0.0.1',
        'REDIS_PORT': '6379',
        'REDIS_DB': '0',
    })

    output = backend.render_templates()

    assert list(output.keys()) == ['dask-kubernetes-configmap.yaml', 'dask-kubernetes-service.yaml', 'dask-kubernetes-pod.yaml']

def test_validate_workflow_specify_variable_not_found(mocker):
    pr1 = cwt.Variable('file:///test1.nc', 'pr')
    pr2 = cwt.Variable('file:///test2.nc', 'prw')

    sub1 = cwt.Process(identifier='CDAT.subset')
    sub1.add_inputs(pr1)

    sub2 = cwt.Process(identifier='CDAT.subset')
    sub2.add_inputs(pr2)

    merge = cwt.Process(identifier='CDAT.merge')
    merge.add_inputs(sub1, sub2)

    sum = cwt.Process(identifier='CDAT.sum')
    sum.add_inputs(merge)
    sum.add_parameters(variable='clt')

    data_inputs = {
        'variable': [pr1.to_dict(), pr2.to_dict()],
        'domain': [],
        'operation': [sub1.to_dict(), sub2.to_dict(), merge.to_dict(), sum.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs('CDAT.sum', data_inputs)

    mocker.patch.object(context, 'action')

    with pytest.raises(WPSError):
        base.validate_workflow(context)


def test_validate_workflow_specify_variable(mocker):
    pr1 = cwt.Variable('file:///test1.nc', 'pr')
    pr2 = cwt.Variable('file:///test2.nc', 'prw')

    sub1 = cwt.Process(identifier='CDAT.subset')
    sub1.add_inputs(pr1)

    sub2 = cwt.Process(identifier='CDAT.subset')
    sub2.add_inputs(pr2)

    merge = cwt.Process(identifier='CDAT.merge')
    merge.add_inputs(sub1, sub2)

    group = cwt.Process(identifier='CDAT.groupby_bins')
    group.add_inputs(merge)
    group.add_parameters(variable='prw', bins=['10', '20'])

    sum = cwt.Process(identifier='CDAT.sum')
    sum.add_inputs(group)
    sum.add_parameters(variable='pr')

    data_inputs = {
        'variable': [pr1.to_dict(), pr2.to_dict()],
        'domain': [],
        'operation': [sub1.to_dict(), sub2.to_dict(), merge.to_dict(), sum.to_dict(), group.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs('CDAT.sum', data_inputs)

    mocker.patch.object(context, 'action')

    base.validate_workflow(context)


def test_validate_workflow_missmatch_input(mocker):
    pr1 = cwt.Variable('file:///test1.nc', 'pr')
    pr2 = cwt.Variable('file:///test2.nc', 'prw')

    agg = cwt.Process(identifier='CDAT.aggregate')
    agg.add_inputs(pr1, pr2)

    data_inputs = {
        'variable': [pr1.to_dict(), pr2.to_dict()],
        'domain': [],
        'operation': [agg.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs('CDAT.aggregate', data_inputs)

    mocker.patch.object(context, 'action')

    with pytest.raises(base.ValidationError):
        base.validate_workflow(context)


def test_validate_workflow(mocker):
    pr1 = cwt.Variable('file:///test1.nc', 'pr')
    pr2 = cwt.Variable('file:///test2.nc', 'pr')

    agg = cwt.Process(identifier='CDAT.aggregate')
    agg.add_inputs(pr1, pr2)

    max = cwt.Process(identifier='CDAT.max')
    max.add_inputs(agg)
    max.add_parameters(rename=['pr', 'pr_test'])

    data_inputs = {
        'variable': [pr1.to_dict(), pr2.to_dict()],
        'domain': [],
        'operation': [agg.to_dict(), max.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs('CDAT.max', data_inputs)

    mocker.patch.object(context, 'action')

    base.validate_workflow(context)


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
        b'0',
        b'{}',
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
    (b'NO', [], backend.WaitingState),
])
def test_resource_ack_state(mocker, transition, frames, expected):
    b = mocker.MagicMock()

    mocker.patch.object(backend, 'build_workflow')

    state = backend.ResourceAckState('CDAT.subset', '{"variable": [], "domain": [], "operation": []}', '0', '0', '0', '0', '{}')

    frames.insert(0, transition.decode())
    frames.append('{"namespace": "default"}')

    new_state = state.on_event(b, *frames)

    assert isinstance(new_state, expected)


ENV = {
    'IMAGE': 'aims2.llnl.gov/compute-tasks:latest',
    'WORKERS': '8',
    'REDIS_HOST': '127.0.0.1',
    'REDIS_PORT': '6379',
    'REDIS_DB': '0',
}

@pytest.mark.parametrize('transition,patch_env,expected', [
    (backend.REQUEST, ENV, backend.ResourceAckState),
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
        'mounts': [],
    }

    b = mocker.MagicMock()

    state = backend.WaitingState()

    new_state = state.on_event(b, transition.decode(), 'devel', 'CDAT.subset', '{"variable": [], "domain": [], "operation": []}', '0', '0', '0', '0', '{}')

    assert isinstance(new_state, expected)


def test_build_workflow(mocker):
    mocker.patch('compute_tasks.context.operation.OperationContext.action')

    workflow = backend.build_workflow('CDAT.subset', json.dumps(DATA_INPUTS), '0', '0', '0', '0', namespace='default')

    assert workflow


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

