import os

from compute_tasks.context.base_context import BaseContext

def test_build_output_variable(mocker):
    state = BaseContext()

    state.extra = {}

    mocker.patch.object(state, 'build_output')

    state.build_output_variable('tas', name='test_output')

    state.build_output.assert_called_with('nc', 'application/netcdf', var_name='tas', name='test_output')


def test_build_output(mocker):
    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/tmp',
    })

    state = BaseContext()
    state.user = 0
    state.job = 0

    state.track_output = mocker.MagicMock()

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

    state = BaseContext()
    state.user = 0
    state.job = 0

    uuid = mocker.patch('compute_tasks.context.base_context.uuid')

    uuid.uuid4.return_value = '1234'

    exists = mocker.patch('compute_tasks.context.base_context.os.path.exists')
    makedirs = mocker.patch('compute_tasks.context.base_context.os.makedirs')

    exists.return_value = False

    path = state.generate_local_path('nc')

    assert path == '/tmp/0/0/1234.nc'

    exists.assert_called_with('/tmp/0/0')
    makedirs.assert_called_with('/tmp/0/0')


