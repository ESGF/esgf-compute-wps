import cdms2
import cwt
import pytest
import numpy as np

from compute_tasks import managers
from compute_tasks import WPSError


def test_parse_uniform_arg_value():
    start, default_n, delta = managers.parse_uniform_arg('0.5:180:1', 0, 180)

    assert start == 0.5
    assert default_n == 180
    assert delta == 1


def test_parse_uniform_arg():
    start, default_n, delta = managers.parse_uniform_arg('1', 0, 180)

    assert start == 0.5
    assert default_n == 180
    assert delta == 1


def test_subset_grid():
    grid = cdms2.createGaussianGrid(32)

    selector = {
        'lat': (-45, 45),
        'lon': (180, 360),
    }

    output = managers.subset_grid(grid, selector)

    assert output.shape == (16, 33)


def test_copy(esgf_data, mocker):
    target = mocker.MagicMock()

    im = managers.InputManager(esgf_data.fm, [esgf_data.data['tas']['files'][0]], 'tas')

    im.load_variables_and_axes(target)

    output = im.copy()

    assert id(output) != id(im)
    assert im.attrs == output.attrs

    for x in output.vars.keys():
        assert id(output.vars[x]) != id(im.vars[x])

    assert im.vars_axes == output.vars_axes

    for x in output.axes.keys():
        assert id(output.axes[x]) != id(im.axes[x])


def test_remove_axis_removed(esgf_data, mocker):
    target = mocker.MagicMock()

    im = managers.InputManager(esgf_data.fm, [esgf_data.data['tas']['files'][0]], 'tas')

    im.load_variables_and_axes(target)

    im.remove_axis('time')

    assert im.variable_axes == ['lat', 'lon']
    assert 'time' not in im.axes

    with pytest.raises(WPSError):
        im.remove_axis('time')


def test_remove_axis(esgf_data, mocker):
    target = mocker.MagicMock()

    im = managers.InputManager(esgf_data.fm, [esgf_data.data['tas']['files'][0]], 'tas')

    im.load_variables_and_axes(target)

    im.remove_axis('time')

    assert im.variable_axes == ['lat', 'lon']
    assert 'time' not in im.axes


def test_variable_axes(esgf_data, mocker):
    target = mocker.MagicMock()

    im = managers.InputManager(esgf_data.fm, [esgf_data.data['tas']['files'][0]], 'tas')

    im.load_variables_and_axes(target)

    assert im.variable_axes == ['time', 'lat', 'lon']


def test_variable(mocker, esgf_data):
    fm = mocker.MagicMock()

    im = managers.InputManager(fm, [], 'tas')

    variable = esgf_data.to_dask_array('tas')

    im.variable = variable

    assert id(variable) == id(im.vars['tas'])

    v0 = im.variable

    assert id(variable) == id(v0)


def test_blocks(mocker, esgf_data):
    fm = mocker.MagicMock()

    im = managers.InputManager(fm, [], 'tas')

    im.vars['tas'] = esgf_data.to_dask_array('tas')

    assert len([x for x in im.blocks]) > 0


def test_dtype(mocker, esgf_data):
    fm = mocker.MagicMock()

    im = managers.InputManager(fm, [], 'tas')

    im.vars['tas'] = esgf_data.to_dask_array('tas')

    assert im.dtype == np.dtype('float32')


def test_nbytes(mocker, esgf_data):
    fm = mocker.MagicMock()

    im = managers.InputManager(fm, [], 'tas')

    im.vars['tas'] = esgf_data.to_dask_array('tas')

    assert im.nbytes == 807321600


def test_shape(mocker, esgf_data):
    fm = mocker.MagicMock()

    im = managers.InputManager(fm, [], 'tas')

    im.vars['tas'] = esgf_data.to_dask_array('tas')

    assert im.shape == (3650, 192, 288)


def test_from_cwt_variables_mismatched_var_name(mocker):
    fm = mocker.MagicMock()

    v0 = cwt.Variable('file:///test.nc', 'tas')
    v1 = cwt.Variable('file:///test1.nc', 'clt')

    with pytest.raises(WPSError):
        managers.InputManager.from_cwt_variables(fm, [v0, v1])


def test_from_cwt_variables(mocker):
    fm = mocker.MagicMock()

    v0 = cwt.Variable('file:///test.nc', 'tas')
    v1 = cwt.Variable('file:///test1.nc', 'tas')

    im = managers.InputManager.from_cwt_variables(fm, [v0, v1])

    assert im.uris == ['file:///test.nc', 'file:///test1.nc']
    assert im.var_name == 'tas'


def test_from_cwt_variable(mocker):
    fm = mocker.MagicMock()

    v0 = cwt.Variable('file:///test.nc', 'tas')

    im = managers.InputManager.from_cwt_variable(fm, v0)

    assert im.uris == ['file:///test.nc']
    assert im.var_name == 'tas'


def test_repr(mocker):
    fm = mocker.MagicMock()

    expected = "InputManager(uris=['file:///test1.nc'], var_name='tas', attrs={}, vars={}, vars_axes={}, axes={})"

    assert repr(managers.InputManager(fm, ['file:///test1.nc'], 'tas')) == expected
