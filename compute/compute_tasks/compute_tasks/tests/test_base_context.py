import os

from compute_tasks.context.base_context import BaseContext

def test_build_output_variable(mocker):
    state = BaseContext()

    state.extra = {}

    mocker.patch.object(state, 'build_output')

    state.build_output_variable('tas', name='test_output')

    state.build_output.assert_called_with('application/netcdf', var_name='tas', name='test_output')


def test_build_output(mocker):
    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/tmp',
    })

    state = BaseContext()
    state.user = 0
    state.job = 0

    state.track_output = mocker.MagicMock()

    path = state.build_output('application/netcdf', filename='test.nc', var_name='tas', name='test_output')

    state.track_output.assert_called_with('/tmp/0/0/test.nc')

    variable = state.output[0]

    assert variable.uri == '/tmp/0/0/test.nc'
    assert variable.var_name == 'tas'
    assert variable.name == 'test_output'
    assert variable.mime_type == 'application/netcdf'

    assert path == '/tmp/0/0/test.nc'


def test_generate_local_path_override_output(mocker):
    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/tmp',
    })

    state = BaseContext()
    state.user = 0
    state.job = 0
    state.extra = {'output_path': '/data'}

    uuid4 = mocker.patch('compute_tasks.context.base_context.uuid.uuid4', return_value='1234')
    exists = mocker.patch('compute_tasks.context.base_context.os.path.exists', return_value=False)
    makedirs = mocker.patch('compute_tasks.context.base_context.os.makedirs')

    output = state.generate_local_path()

    assert output == '/data/1234.nc'

    exists.asset_called_with('/data')
    makedirs.assert_called_with('/data')


def test_generate_local_path_filename(mocker):
    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/tmp',
    })

    state = BaseContext()
    state.user = 0
    state.job = 0

    uuid4 = mocker.patch('compute_tasks.context.base_context.uuid.uuid4', return_value='1234')
    exists = mocker.patch('compute_tasks.context.base_context.os.path.exists', return_value=False)
    makedirs = mocker.patch('compute_tasks.context.base_context.os.makedirs')

    output = state.generate_local_path('CDAT.workflow_output.nc')

    assert output == '/tmp/0/0/CDAT.workflow_output.nc'

    exists.asset_called_with('/tmp/0/0')
    makedirs.assert_called_with('/tmp/0/0')


def test_generate_local_path(mocker):
    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/tmp',
    })

    state = BaseContext()
    state.user = 0
    state.job = 0

    uuid4 = mocker.patch('compute_tasks.context.base_context.uuid.uuid4', return_value='1234')
    exists = mocker.patch('compute_tasks.context.base_context.os.path.exists', return_value=False)
    makedirs = mocker.patch('compute_tasks.context.base_context.os.makedirs')

    output = state.generate_local_path()

    assert output == '/tmp/0/0/1234.nc'

    exists.asset_called_with('/tmp/0/0')
    makedirs.assert_called_with('/tmp/0/0')
