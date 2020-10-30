import datetime

import cwt
import pytest

from compute_tasks import celery_app
from compute_tasks import context


def test_import_handlers(mocker):
    base = mocker.patch('compute_tasks.base')

    celery_app.import_modules_handler()

    base.discover_processes.assert_called()


def test_byteify():
    data = {
        b'data': {
            b'items': [b'one', b'two'],
            b'data': b'hello',
        },
    }

    output = celery_app.byteify(data)

    assert isinstance(list(output.keys())[0], str)
    assert isinstance(list(output['data'].keys())[0], str)
    assert isinstance(list(output['data'].keys())[1], str)
    assert isinstance(output['data']['items'][0], str)
    assert isinstance(output['data']['items'][1], str)
    assert isinstance(output['data']['data'], str)


def test_default_unknown_type():
    data = {}

    with pytest.raises(TypeError):
        celery_app.default(data)


def test_encoder_decoder():
    now = datetime.datetime.now()

    data = {
        'slice_data': slice(10, 20, 2),
        'variable_data': cwt.Variable('file:///test.nc', 'tas'),
        'domain_data': cwt.Domain(time=slice(10, 20, 2)),
        'process_data': cwt.Process(identifier='CDAT.subset'),
        'timedelta_data': datetime.timedelta(seconds=60),
        'datetime_data': now,
        'operation_context_data': context.OperationContext(0, 0, 0, 0, variable={}, domain={}, operation={}),
    }

    encoded = celery_app.encoder(data)

    decoded = celery_app.decoder(encoded)

    assert isinstance(decoded['slice_data'], slice)
    assert isinstance(decoded['variable_data'], cwt.Variable)
    assert isinstance(decoded['domain_data'], cwt.Domain)
    assert isinstance(decoded['process_data'], cwt.Process)
    assert isinstance(decoded['timedelta_data'], datetime.timedelta)
    assert isinstance(decoded['datetime_data'], datetime.datetime)
    assert isinstance(decoded['operation_context_data'], context.OperationContext)
