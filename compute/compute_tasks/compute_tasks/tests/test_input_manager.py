import cdms2
import cwt
import dask
import pytest
import numpy as np

from compute_tasks import managers
from compute_tasks import WPSError


def test_format_dimension_unknown_crs():
    with pytest.raises(WPSError):
        managers.format_dimension(cwt.Dimension('time', 0, 1, crs=cwt.CRS('new')))


@pytest.mark.parametrize('dim,expected', [
    (cwt.Dimension('time', 10, 200, crs=cwt.VALUES), (10, 200, 1)),
    (cwt.Dimension('time', 10, 200, crs=cwt.INDICES), slice(10, 200, 1)),
    (cwt.Dimension('time', '01-01-2001', '01-01-2101', crs=cwt.TIMESTAMPS), ('01-01-2001', '01-01-2101', 1)),

])
def test_format_dimension(dim, expected):
    output = managers.format_dimension(dim)

    assert output == expected


@pytest.mark.parametrize('domain,expected', [
    (None, {}),
    (cwt.Domain(time=(100, 200)), {'time': (100, 200, 1)}),
])
def test_domain_to_dict(domain, expected):
    output = managers.domain_to_dict(domain)

    assert output == expected


@pytest.mark.parametrize('dim,expected', [
    ((674895.0, 674985.0, 2), slice(10, 101, 2)),
    ((674895.0, 674995.0), slice(10, 111, 1)),
    (slice(10, 100), slice(10, 100)),
    ((674895.0, 674985.0), slice(10, 101, 1))
])
def test_map_dimension(esgf_data, dim, expected):
    tas = esgf_data.to_cdms2('tas')['tas']

    axis = tas.getTime()

    output = managers.map_dimension(dim, axis)

    assert output == expected


def test_map_domain(esgf_data):
    tas = esgf_data.to_cdms2('tas')['tas']

    domain = cwt.Domain(time=(100, 200), lat=(-45, 45))

    axes = dict((x.id, x) for x in tas.getAxisList())

    output = managers.map_domain(domain, axes)

    assert list(output.keys()) == ['time', 'lat', 'lon']
    assert output['time'] is None
    assert output['lat'] == slice(48, 144, 1)
    assert output['lon'] == slice(None, None, None)


def test_sort_uris_error_determine_order(esgf_data):
    esgf_data.to_local_path('tas', file_index=0)
    esgf_data.to_local_path('tas', file_index=1)

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    ordering = [
            ('file:///test2.nc', 'days since 0001-01-01 00:00:00', 678535.0),
            ('file:///test1.nc', 'days since 0001-01-01 00:00:00', 678535.0),
    ]

    with pytest.raises(WPSError):
        im.sort_uris(ordering)


def test_sort_uris_by_units(esgf_data):
    esgf_data.to_local_path('tas', file_index=0)
    esgf_data.to_local_path('tas', file_index=1)

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    ordering = [
            ('file:///test2.nc', 'days since 1001-01-01 00:00:00', 678535.0),
            ('file:///test1.nc', 'days since 0001-01-01 00:00:00', 678535.0),
    ]

    im.sort_uris(ordering)

    assert im.uris[0] == 'file:///test1.nc'
    assert im.uris[1] == 'file:///test2.nc'


def test_sort_uris_by_firsts(esgf_data):
    esgf_data.to_local_path('tas', file_index=0)
    esgf_data.to_local_path('tas', file_index=1)

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    ordering = [
            ('file:///test2.nc', 'days since 0001-01-01 00:00:00', 678535.0),
            ('file:///test1.nc', 'days since 0001-01-01 00:00:00', 674885.0),
    ]

    im.sort_uris(ordering)

    assert im.uris[0] == 'file:///test1.nc'
    assert im.uris[1] == 'file:///test2.nc'


def test_gather_temporal_info_missing_time(esgf_data, mocker):
    mocker.patch.object(esgf_data.fm, 'get_variable')

    esgf_data.fm.get_variable.return_value.getTime.side_effect = [
        mocker.MagicMock(),
        None,
    ]

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    with pytest.raises(WPSError):
        im.gather_temporal_info()


def test_gather_temporal_info(esgf_data, mocker):
    esgf_data.to_local_path('tas', file_index=0)
    esgf_data.to_local_path('tas', file_index=1)

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    temporal_info = im.gather_temporal_info()

    assert temporal_info[0] == (esgf_data.data['tas']['files'][0], 'days since 0001-01-01 00:00:00', 674885.0)
    assert temporal_info[1] == (esgf_data.data['tas']['files'][1], 'days since 0001-01-01 00:00:00', 678535.0)


def test_subset_from_delayed(esgf_data, mocker):
    esgf_data.to_local_path('tas', file_index=0)
    esgf_data.to_local_path('tas', file_index=1)

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    mocker.patch.object(im.fm, 'requires_cert')

    im.fm.requires_cert.return_value = True

    data, subset_data = im.subset(None)

    assert data.shape == (7300, 192, 288)
    assert subset_data.shape == (7300, 192, 288)


def test_subset_single(esgf_data, mocker):
    esgf_data.to_local_path('tas', file_index=0)

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'][:1], 'tas')

    data, subset_data = im.subset(None)

    assert data.shape == (3650, 192, 288)
    assert subset_data.shape == (3650, 192, 288)


def test_subset_multiple(esgf_data, mocker):
    esgf_data.to_local_path('tas', file_index=0)
    esgf_data.to_local_path('tas', file_index=1)

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    data, subset_data = im.subset(None)

    assert data.shape == (7300, 192, 288)
    assert subset_data.shape == (7300, 192, 288)


def test_to_xarray(esgf_data, mocker):
    esgf_data.to_local_path('tas', file_index=0)
    esgf_data.to_local_path('tas', file_index=1)

    tas_da1 = esgf_data.to_dask_array('tas', file_index=0)
    tas_da2 = esgf_data.to_dask_array('tas', file_index=1)

    tas_da = dask.array.concatenate([tas_da1, tas_da2])

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    im.subset_variables_and_axes(None, tas_da)

    xr_ds = im.to_xarray()

    expected_axes = ['time', 'lat', 'lon', 'tas', 'nbnd', 'time_bnds', 'lat_bnds', 'lon_bnds']

    assert sorted([x for x in xr_ds.variables.keys()]) == sorted(expected_axes)


@pytest.mark.parametrize('domain,expected_shape', [
    (None, (7300, 192, 288)),
    (cwt.Domain(time=slice(100, 500)), (400, 192, 288)),
    (cwt.Domain(lat=(-45, 45), lon=(90, 270)), (7300, 96, 145)),
])
def test_subset_variables_and_axes(esgf_data, mocker, domain, expected_shape):
    esgf_data.to_local_path('tas', file_index=0)
    esgf_data.to_local_path('tas', file_index=1)

    tas_da1 = esgf_data.to_dask_array('tas', file_index=0)
    tas_da2 = esgf_data.to_dask_array('tas', file_index=1)

    tas_da = dask.array.concatenate([tas_da1, tas_da2])

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    target = im.subset_variables_and_axes(domain, tas_da)

    assert target.shape == expected_shape


def test_load_variables_and_axes_multiple(esgf_data, mocker):
    mocker.spy(managers.input_manager.cdms2, 'createVariable')
    mocker.spy(managers.input_manager.cdms2.MV2, 'axisConcatenate')

    esgf_data.to_local_path('tas', file_index=0)
    esgf_data.to_local_path('tas', file_index=1)

    tas_da1 = esgf_data.to_dask_array('tas', file_index=0)
    tas_da2 = esgf_data.to_dask_array('tas', file_index=1)

    tas_da = dask.array.concatenate([tas_da1, tas_da2])

    im = managers.InputManager(esgf_data.fm, esgf_data.data['tas']['files'], 'tas')

    im.load_variables_and_axes(tas_da)

    assert len(im.vars) == 4
    assert len(im.attrs) == 9
    assert id(im.vars['tas']) == id(tas_da)

    managers.input_manager.cdms2.createVariable.assert_called()
    managers.input_manager.cdms2.MV2.axisConcatenate.assert_called()

    assert im.axes['time'].shape == (7300, )


def test_load_variables_and_axes_single(esgf_data, mocker):
    mocker.spy(managers.input_manager.cdms2, 'createVariable')
    mocker.spy(managers.input_manager.cdms2.MV2, 'axisConcatenate')

    tas_da = esgf_data.to_dask_array('tas')

    im = managers.InputManager(esgf_data.fm, [esgf_data.data['tas']['files'][0]], 'tas')

    im.load_variables_and_axes(tas_da)

    assert len(im.vars) == 4
    assert len(im.attrs) == 9

    managers.input_manager.cdms2.createVariable.assert_not_called()
    managers.input_manager.cdms2.MV2.axisConcatenate.assert_not_called()


def test_load_variables(esgf_data):
    tas = esgf_data.to_cdms2('tas')

    tas_da = esgf_data.to_dask_array('tas')

    im = managers.InputManager(esgf_data.fm, [esgf_data.data['tas']['files'][0]], 'tas')

    im.load_variables(tas, tas_da)

    assert len(im.vars) == 4
    assert len(im.attrs) == 8
    assert id(im.vars['tas']) == id(tas_da)


def test_load_axes(esgf_data):
    tas = esgf_data.to_cdms2('tas')

    im = managers.InputManager(esgf_data.fm, [esgf_data.data['tas']['files'][0]], 'tas')

    im.load_axes(tas['tas'], False)

    assert im.target_var_axes == ['time', 'lat', 'lon']
    assert im.units == tas['tas'].getTime().units
    assert len(im.vars) == 0
    assert len(im.time_axis) == 1
    assert len(im.attrs) == 3
    assert len(im.vars_axes) == 1
    assert len(im.vars_axes['tas']) == 3


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


def test_slice_to_subaxis_empty():
    axis = dask.array.random.random((320, 2, 2))

    i, j, k = managers.slice_to_subaxis(slice(None, None, None), axis)

    assert i == 0
    assert j == 320
    assert k == 1


def test_slice_to_subaxis():
    axis = dask.array.random.random((320, 2, 2))

    i, j, k = managers.slice_to_subaxis(slice(10, 20, 2), axis)

    assert i == 10
    assert j == 20
    assert k == 2


def test_from_delayed(esgf_data, mocker):
    esgf_data.cert_data = ''

    var = esgf_data.to_cdms2('tas')['tas']

    output = managers.from_delayed(esgf_data, 'file:///test.nc', var, (100, 192, 288))

    assert isinstance(output, dask.array.Array)
    assert output.shape == var.shape
    assert output.dtype == var.dtype


def test_new_shape():
    shape = managers.new_shape((3650, 192, 288), slice(10, 20, 1))

    assert shape == (10, 192, 288)


@pytest.mark.skip(reason='Disabled regridding')
def test_generate_grid_from_file(esgf_data):
    gridder = cwt.Gridder(grid=cwt.Variable(esgf_data.to_local_path('tas'), 'tas'))

    selector = {
        'lat': (-45, 45),
        'lon': (90, 270),
    }

    grid = managers.generate_grid(gridder, selector)

    assert grid.getType() == 'generic'
    assert grid.shape == (96, 145)


@pytest.mark.skip(reason='Disabled regridding')
def test_generate_grid():
    gridder = cwt.Gridder(grid='gaussian~32')

    selector = {
        'lat': (-45, 45),
        'lon': (90, 270),
    }

    grid = managers.generate_grid(gridder, selector)

    assert grid.getType() == 'gaussian'
    assert grid.shape == (16, 33)


def test_read_grid_from_file_error_reading(esgf_data):
    gridder = cwt.Gridder(grid=cwt.Variable('file:///test.nc', 'tas'))

    with pytest.raises(WPSError):
        managers.read_grid_from_file(gridder)


def test_read_grid_from_file(esgf_data):
    uri = esgf_data.to_local_path('tas')

    gridder = cwt.Gridder(grid=cwt.Variable(uri, 'tas'))

    grid = managers.read_grid_from_file(gridder)

    assert grid.getType() == 'generic'
    assert grid.shape == (192, 288)


def test_generate_user_defined_grid_missing_parameters():
    with pytest.raises(WPSError):
        managers.generate_user_defined_grid(cwt.Gridder(grid='gaussian'))


def test_generate_user_defined_grid_no_grid():
    grid = managers.generate_user_defined_grid(None)

    assert grid is None


def test_generate_user_defined_grid_uniform_bad_format():
    with pytest.raises(WPSError):
        managers.generate_user_defined_grid(cwt.Gridder(grid='uniform~-45:180:1'))


def test_generate_user_defined_grid_uniform():
    grid = managers.generate_user_defined_grid(cwt.Gridder(grid='uniform~-45:180:1x0:360:1'))

    assert grid.getType() == 'uniform'
    assert grid.shape == (180, 360)


def test_generate_user_defined_grid_unknown():
    with pytest.raises(WPSError):
        managers.generate_user_defined_grid(cwt.Gridder(grid='cubic~32'))


def test_generate_user_defined_grid_gaussian_bad_format():
    with pytest.raises(WPSError):
        managers.generate_user_defined_grid(cwt.Gridder(grid='gaussian~32.0'))


def test_generate_user_defined_grid_gaussian():
    grid = managers.generate_user_defined_grid(cwt.Gridder(grid='gaussian~32'))

    assert grid.getType() == 'gaussian'
    assert grid.shape == (32, 64)


@pytest.mark.parametrize('test_input', [
    '-0.5:180:',
    ':180:1',
    '::',
    '0.5::1',
])
def test_parse_uniform_arg_error_parsing(test_input):
    with pytest.raises(WPSError):
        managers.parse_uniform_arg(test_input)


def test_parse_uniform_arg():
    start, default_n, delta = managers.parse_uniform_arg('-0.5:180:1')

    assert start == -0.5
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
