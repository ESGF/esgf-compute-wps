import os
import cwt

from compute_tasks.context.base_context import BaseContext

def test_topo_sort(mocker):
    v0 = cwt.Variable('file:///test1.nc', 'pr', name='v0')
    v1 = cwt.Variable('file:///test2.nc', 'prw', name='v1')

    s0 = cwt.Process(identifier='CDAT.subset', inputs=v0, name='s0')
    s1 = cwt.Process(identifier='CDAT.subset', inputs=v1, name='s1')

    merge = cwt.Process(identifier='CDAT.merge', inputs=[s0, s1], name='merge')

    max = cwt.Process(identifier='CDAT.max', inputs=merge, name='max')
    max.add_parameters(rename=['pr', 'pr_max'])

    v = {v0.name: v0, v1.name: v1}
    o = {s0.name: s0, s1.name: s1, merge.name: merge, max.name: max}

    base = BaseContext(variable=v, operation=o)

    expected = (
        ('s0', ['pr']),
        ('s1', ['prw']),
        ('merge', ['prw', 'pr']),
        ('max', ['pr', 'prw']),
    )

    for x, y in zip(base.topo_sort(), expected):
        id, vars = x
        exp_id, exp_vars = y

        assert id.name == exp_id
        assert sorted(vars) == sorted(exp_vars)


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
