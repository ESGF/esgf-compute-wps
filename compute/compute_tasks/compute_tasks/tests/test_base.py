import cwt
import pytest

from compute_tasks import base
from compute_tasks import tests
from compute_tasks import WPSError
from compute_tasks import context


def test_validate_parameter():
    param = cwt.NamedParameter('axes', 'time')
    param2 = cwt.NamedParameter('axes')
    param3 = cwt.NamedParameter('axes', 'time', 'lat', 'lon')
    param4 = cwt.NamedParameter('axes', '3.0')

    def validate_true(**kwargs):
        return True

    def validate_false(**kwargs):
        return False

    def validate_exception(**kwargs):
        raise Exception('bad')

    def validate_validationerror(**kwargs):
        raise base.ValidationError('bad')

    base.validate_parameter(param, 'axes', list, str, 1, float('inf'), validate_true, 1, ['tas',])

    base.validate_parameter(param, 'axes', list, str, 1, float('inf'), None, 1, ['tas',])

    with pytest.raises(base.ValidationError):
        base.validate_parameter(None, 'axes', list, str, 1, float('inf'), None, 1, ['tas',])

    with pytest.raises(base.ValidationError):
        base.validate_parameter(param, 'axes', list, str, 1, float('inf'), validate_exception, 1, ['tas',])

    with pytest.raises(base.ValidationError):
        base.validate_parameter(param, 'axes', list, str, 1, float('inf'), validate_validationerror, 1, ['tas',])

    with pytest.raises(base.ValidationError):
        base.validate_parameter(param, 'axes', list, str, 1, float('inf'), validate_false, 1, ['tas',])

    with pytest.raises(base.ValidationError):
        base.validate_parameter(param2, 'axes', list, str, 1, float('inf'), None, 1, ['tas',])

    with pytest.raises(base.ValidationError):
        base.validate_parameter(param3, 'axes', list, str, 1, 2, None, 1, ['tas',])

    with pytest.raises(base.ValidationError):
        base.validate_parameter(param4, 'axes', list, int, 1, float('inf'), None, 1, ['tas',])

    with pytest.raises(base.ValidationError):
        base.validate_parameter(param4, 'axes', int, None, 1, float('inf'), None, 1, ['tas',])

def test_parameter_decorator():
    def validate():
        pass

    def process():
        pass

    process._parameters = {}

    base.parameter('axes', 'Description', list, str, min=1, max=float('inf'), validate_func=validate)(process)

    assert process._parameters['axes']['name'] == 'axes'
    assert process._parameters['axes']['desc'] == 'Description'
    assert process._parameters['axes']['type'] == list
    assert process._parameters['axes']['subtype'] == str
    assert process._parameters['axes']['min'] == 1
    assert process._parameters['axes']['max'] == float('inf')
    assert process._parameters['axes']['validate_func'] == validate


def test_build_parameter():
    def validate():
        pass

    param = base.build_parameter('axes', 'Description', list, str, min=1, max=float('inf'), validate_func=validate)

    assert param['name'] == 'axes'
    assert param['desc'] == 'Description'
    assert param['type'] == list
    assert param['subtype'] == str
    assert param['min'] == 1
    assert param['max'] == float('inf')
    assert param['validate_func'] == validate


def test_register_process():
    @base.register_process('CDAT.mathstuff45', abstract='abstract data', version='1.0.0', min=10, extra_data='extra_data_content')
    def test_task(self, ctx):
        return ctx

    registry_entry = {
        'identifier': 'CDAT.mathstuff45',
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
    ctx = context.OperationContext(0, 0, 0, 0, variable={}, domain={}, operation={})

    mocker.patch.object(ctx, 'failed')
    mocker.patch.object(ctx, 'state')

    ctx.failed.side_effect = WPSError()

    base_task = base.CWTBaseTask()

    e = Exception('Error')

    with pytest.raises(WPSError):
        base_task.on_failure(e, 0, (ctx,), {}, None)

    ctx.failed.assert_called_with(0, str(e))


def test_cwt_base_task_failure(mocker):
    ctx = context.OperationContext(0, 0, 0, 0, variable={}, domain={}, operation={})

    mocker.patch.object(ctx, 'failed')
    mocker.patch.object(ctx, 'state')

    base_task = base.CWTBaseTask()

    e = Exception('Error')

    base_task.on_failure(e, 0, (ctx,), {}, None)

    ctx.failed.assert_called_with(0, str(e))


def test_cwt_base_task_retry_error(mocker):
    ctx = context.OperationContext(0, 0, 0, 0, variable={}, domain={}, operation={})

    mocker.patch.object(ctx, 'message')
    mocker.patch.object(ctx, 'state')

    ctx.message.side_effect = WPSError()

    base_task = base.CWTBaseTask()

    e = Exception('Error')

    with pytest.raises(WPSError):
        base_task.on_retry(e, 0, (ctx,), {}, None)

    ctx.message.assert_called_with(0, 'Retrying from error: Error')


def test_cwt_base_task_retry(mocker):
    ctx = context.OperationContext(0, 0, 0, 0, variable={}, domain={}, operation={})

    mocker.patch.object(ctx, 'message')
    mocker.patch.object(ctx, 'state')

    base_task = base.CWTBaseTask()

    e = Exception('Error')

    base_task.on_retry(e, 0, (ctx,), {}, None)

    ctx.message.assert_called_with(0, 'Retrying from error: Error')
