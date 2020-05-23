import pytest

from compute_tasks import base
from compute_tasks import tests
from compute_tasks import WPSError


def test_register_process():
    @base.register_process('CDAT.mathstuff45', abstract='abstract data', version='1.0.0', min=10, extra_data='extra_data_content')
    def test_task(self, context):
        return context

    registry_entry = {
        'identifier': 'CDAT.mathstuff45',
        'backend': 'CDAT',
        'abstract': 'abstract data',
        'metadata': '{"extra_data": "extra_data_content", "inputs": 10}',
        'version': '1.0.0',
    }

    assert 'CDAT.mathstuff45' in base.REGISTRY
    assert base.REGISTRY['CDAT.mathstuff45'] == registry_entry
    assert base.BINDINGS['CDAT.mathstuff45'] == test_task


def test_discover_processes():
    processes = base.discover_processes()

    assert len(processes) > 0


def test_get_process_unknown_process(mocker):
    mocker.patch.dict(base.BINDINGS, {
    }, True)

    with pytest.raises(WPSError):
        base.get_process('CDAT.subset')


def test_get_process(mocker):
    partial_func = mocker.MagicMock()
    mocker.patch.dict(base.BINDINGS, {
        'CDAT.subset': partial_func,
    }, True)

    output = base.get_process('CDAT.subset')

    assert output == partial_func


def test_cwt_base_task_failure_error(mocker):
    context = mocker.MagicMock()

    context.failed.side_effect = WPSError()

    base_task = base.CWTBaseTask()

    e = Exception('Error')

    with pytest.raises(WPSError):
        base_task.on_failure(e, 0, (context,), {}, None)

    context.failed.assert_called_with(str(e))


def test_cwt_base_task_failure(mocker):
    context = mocker.MagicMock()

    base_task = base.CWTBaseTask()

    e = Exception('Error')

    base_task.on_failure(e, 0, (context,), {}, None)

    context.failed.assert_called_with(str(e))

    context.update_metrics.assert_called()


def test_cwt_base_task_retry_error(mocker):
    context = mocker.MagicMock()

    context.message.side_effect = WPSError()

    base_task = base.CWTBaseTask()

    e = Exception('Error')

    with pytest.raises(WPSError):
        base_task.on_retry(e, 0, (context,), {}, None)

    context.message.assert_called_with('Retrying from error: {!s}', e)


def test_cwt_base_task_retry(mocker):
    context = mocker.MagicMock()

    base_task = base.CWTBaseTask()

    e = Exception('Error')

    base_task.on_retry(e, 0, (context,), {}, None)

    context.message.assert_called_with('Retrying from error: {!s}', e)
