import builtins
import json
import re
import os
import tempfile

import cdms2
import cftime
import cwt
import dask
import pytest
import pandas as pd
import numpy as np
import dask.array as da
import xarray as xr
from OpenSSL import SSL
from distributed.utils_test import client
from distributed.utils_test import loop
from distributed.utils_test import cluster_fixture

from jinja2 import Environment, BaseLoader
from myproxy.client import MyProxyClient

from compute_tasks import base
from compute_tasks import cdat
from compute_tasks import WPSError
from compute_tasks.context import operation


MyProxyClient.SSL_METHOD = SSL.TLSv1_2_METHOD

CMIP5_CLT = 'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esg_dataroot/AR5/CMIP5/output/CCCma/CanAM4/amip/3hr/atmos/clt/r1i1p1/clt_cf3hr_CanAM4_amip_r1i1p1_197901010300-201001010000.nc'
CMIP6_CLT = 'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/abrupt-4xCO2/r1i1p1f1/Amon/clt/gn/v20190429/clt_Amon_CanESM5_abrupt-4xCO2_r1i1p1f1_gn_185001-200012.nc'

CMIP6_AGG1 = 'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/esm-piControl/r1i1p1f1/Amon/clt/gn/v20190429/clt_Amon_CanESM5_esm-piControl_r1i1p1f1_gn_530101-540012.nc'
CMIP6_AGG2 = 'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/esm-piControl/r1i1p1f1/Amon/clt/gn/v20190429/clt_Amon_CanESM5_esm-piControl_r1i1p1f1_gn_540101-560012.nc'
CMIP6_AGG3 = 'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/esm-piControl/r1i1p1f1/Amon/clt/gn/v20190429/clt_Amon_CanESM5_esm-piControl_r1i1p1f1_gn_560101-580012.nc'
CMIP6_AGG4 = 'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esgC_dataroot/AR6/CMIP6/CMIP/CCCma/CanESM5/esm-piControl/r1i1p1f1/Amon/clt/gn/v20190429/clt_Amon_CanESM5_esm-piControl_r1i1p1f1_gn_580101-600012.nc'

CMIP5_AGG1 = 'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esg_dataroot/AR5/CMIP5/output/CCCma/CanCM4/decadal1960/day/atmos/tas/r10i1p1/tas_day_CanCM4_decadal1960_r10i1p1_19610101-19701231.nc'
CMIP5_AGG2 = 'http://crd-esgf-drc.ec.gc.ca/thredds/dodsC/esg_dataroot/AR5/CMIP5/output/CCCma/CanCM4/decadal1960/day/atmos/tas/r10i1p1/tas_day_CanCM4_decadal1960_r10i1p1_19710101-19801231.nc'

class TestDataGenerator(object):
    def standard(self, value, lat=90, lon=180, time=10, name=None, time_start=None):
        if name is None:
            name = 'pr'

        if time_start is None:
            time_start = '1990-01-01'

        data_vars = {
            name: (['time', 'lat', 'lon'], np.full((time, lat, lon), value)),
        }

        coords = {
            'time': pd.date_range(time_start, periods=time),
            'lat': (['lat'], np.arange(-90, 90, 180/lat)),
            'lon': (['lon'], np.arange(0, 360, 360/lon)),
        }

        return xr.Dataset(data_vars, coords)

    def increasing_lat(self, lat=90, lon=180, time=10, name=None):
        if name is None:
            name = 'pr'

        data = np.array([np.array([np.full((lon), x) for x in range(lat)]) for _ in range(time)])

        data_vars = {
            name: (['time', 'lat', 'lon'], data),
        }

        coords = {
            'time': pd.date_range('1990-01-01', periods=time),
            'lat': (['lat'], np.arange(-90, 90, 180/lat)),
            'lon': (['lon'], np.arange(0, 360, 360/lon)),
        }

        return xr.Dataset(data_vars, coords)

@pytest.fixture
def test_data():
    return TestDataGenerator()


@pytest.fixture(scope='session')
def mpc():
    m = MyProxyClient(hostname=os.environ['MPC_HOST'])

    cert = m.logon(os.environ['MPC_USERNAME'], os.environ['MPC_PASSWORD'], bootstrap=True)

    return ''.join([x.decode() for x in cert])


def test_groupby_bins_invalid_bin(test_data, mocker):
    identifier = 'CDAT.groupby_bins'

    v1 = test_data.increasing_lat(name='pr')

    inputs = [v1]

    p = cwt.Process(identifier)
    p.add_inputs(cwt.Variable('test.nc', 'pr'))
    p.add_parameters(variable='pr', bins=[str(x) for x in range(0, 90, 10)]+['abcd',])

    data_inputs = {
        'variable': [x.to_dict() for x in p.inputs],
        'domain': [],
        'operation': [p.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs(identifier, data_inputs)

    mocker.patch.object(context, 'message')

    with pytest.raises(WPSError):
        cdat.PROCESS_FUNC_MAP[identifier](context, p, *inputs)


def test_groupby_bins_missing_variable(test_data, mocker):
    identifier = 'CDAT.groupby_bins'

    v1 = test_data.increasing_lat(name='pr')

    inputs = [v1]

    p = cwt.Process(identifier)
    p.add_inputs(cwt.Variable('test.nc', 'pr'))
    p.add_parameters(variable='tas', bins=[str(x) for x in range(0, 90, 10)])

    data_inputs = {
        'variable': [x.to_dict() for x in p.inputs],
        'domain': [],
        'operation': [p.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs(identifier, data_inputs)

    mocker.patch.object(context, 'message')

    with pytest.raises(WPSError):
        cdat.PROCESS_FUNC_MAP[identifier](context, p, *inputs)


def test_groupby_bins(test_data, mocker):
    identifier = 'CDAT.groupby_bins'

    v1 = test_data.increasing_lat(name='pr')

    inputs = [v1]

    p = cwt.Process(identifier)
    p.add_inputs(cwt.Variable('test.nc', 'pr'))
    p.add_parameters(variable='pr', bins=[str(x) for x in range(0, 90, 10)])

    data_inputs = {
        'variable': [x.to_dict() for x in p.inputs],
        'domain': [],
        'operation': [p.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs(identifier, data_inputs)
    context.input_var_names = {p.name: ['pr']}

    mocker.patch.object(context, 'message')

    output = cdat.PROCESS_FUNC_MAP[identifier](context, p, *inputs)

    assert len(output) == 8


@pytest.mark.parametrize('cond', [
    'lat>-45',
    'lat>45',
    'lon>360',
    'lon>=360',
    'lon<180',
    'lon<=180',
    'lon==180',
    'lon!=180',
    pytest.param('lon>>>>180', marks=pytest.mark.xfail),
    pytest.param('lon>>180', marks=pytest.mark.xfail),
    pytest.param(None, marks=pytest.mark.xfail),
    pytest.param('tas>-45', marks=pytest.mark.xfail),
])
def test_where(test_data, cond, mocker):
    identifier = 'CDAT.where'

    v1 = test_data.standard(1, name='pr')

    inputs = [v1]

    p = cwt.Process(identifier)
    p.add_inputs(cwt.Variable('test.nc', 'pr'))

    if cond is not None:
        p.add_parameters(cond=cond)

    data_inputs = {
        'variable': [x.to_dict() for x in p.inputs],
        'domain': [],
        'operation': [p.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs(identifier, data_inputs)
    context.input_var_names = {p.name: ['pr']}

    mocker.patch.object(context, 'message')

    result = cdat.PROCESS_FUNC_MAP[identifier](context, p, *inputs)

    assert 'pr' in result


def test_abstracts():
    for x in cdat.PROCESS_FUNC_MAP.keys():
        assert x in cdat.ABSTRACT


def test_merge(test_data, mocker):
    identifier = 'CDAT.merge'

    v1 = test_data.standard(1, name='pr')
    v2 = test_data.standard(1, name='prw')

    inputs = [v1, v2]

    p = cwt.Process(identifier)
    p.add_inputs(cwt.Variable('test.nc', 'pr'), cwt.Variable('test.nc', 'prw'))

    data_inputs = {
        'variable': [x.to_dict() for x in p.inputs],
        'domain': [],
        'operation': [p.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs(identifier, data_inputs)

    mocker.patch.object(context, 'message')

    result = cdat.PROCESS_FUNC_MAP[identifier](context, p, *inputs)

    assert 'pr' in result
    assert 'prw' in result


def _build_data(test_data, input):
    if isinstance(input, tuple):
        bins = input[1].pop('bins', None)

        if isinstance(input[0], str) and input[0] == 'increasing_lat':
            data = test_data.increasing_lat(**input[1])
        else:
            data = test_data.standard(input[0], **input[1])
    else:
        data = test_data.standard(input)

    return data


@pytest.mark.parametrize('output_name', [None, 'pr_test'])
@pytest.mark.parametrize('identifier,v1,v2,output,extra', [
    ('CDAT.abs', -2, None, 2, {}),
    ('CDAT.add', 1, 2, 3, {}),
    pytest.param('CDAT.add', 1, None, 3, {}, marks=pytest.mark.xfail),
    ('CDAT.add', 1, None, 3, {'const': '2'}),
    ('CDAT.divide', 1, 2, 0.5, {}),
    ('CDAT.divide', 1, None, 0.5, {'const': '2'}),
    ('CDAT.exp', 1, None, 2.718281828459045, {}),
    ('CDAT.log', 5, None, 1.6094379124341003, {}),
    ('CDAT.multiply', 2, 3, 6, {}),
    ('CDAT.multiply', 2, None, 6, {'const': '3'}),
    ('CDAT.power', 2, 2, 4, {}),
    ('CDAT.power', 2, None, 4, {'const': '2'}),
    ('CDAT.subtract', 2, 1, 1, {}),
    ('CDAT.subtract', 2, None, 3, {'const': '-1'}),
    pytest.param('CDAT.subtract', 2, None, 3, {'const': 'abcd'}, marks=pytest.mark.xfail),
    ('CDAT.count', 2, None, np.array(162000), {}),
    ('CDAT.mean', 2, None, np.array(2.0), {}),
    ('CDAT.mean', 2, None, np.full((90, 180), 2), {'axes': ['time']}),
    ('CDAT.std', 5, None, np.full((90, 180), 0), {'axes': ['time']}),
    ('CDAT.var', 5, None, np.full((90, 180), 0), {'axes': ['time']}),
    ('CDAT.squeeze', (5, {'time': 10, 'lat': 1, 'lon': 1}), None, np.full((10,), 5), {}),
    ('CDAT.aggregate', (5, {'time_start': '1990'}), (5, {'time_start': '1991'}), np.full((20, 90, 180), 5), {}),
    ('CDAT.filter_map', ('increasing_lat', {}), None, np.array(106200), {'cond': 'pr>30', 'func': 'count'}),
    ('CDAT.filter_map', ('increasing_lat', {'bins': np.arange(0, 90, 10).tolist()}), None, np.array(106200), {'cond': 'pr>30', 'func': 'count'}),
    ('CDAT.where', ('increasing_lat', {}), None, ('mean', np.array(3.4444444444444443e+19)), {'cond': 'pr>30', 'other': '1e20'}),
    ('CDAT.where', ('increasing_lat', {}), None, ('mean', np.array(60.0)), {'cond': 'pr>30'}),
])
def test_processing(mocker, test_data, identifier, v1, v2, output, extra, output_name):
    inputs = [_build_data(test_data, v1)]

    if v2 is not None:
        inputs.append(_build_data(test_data, v2))

    p = cwt.Process(identifier)
    p.add_parameters(**extra)

    if output_name is not None:
        p.add_parameters(output=output_name)

    vars = [cwt.Variable('test{!s}.nc'.format(str(x)), 'pr') for x, _ in enumerate(inputs)]

    data_inputs = {
        'variable': [x.to_dict() for x in vars],
        'domain': [],
        'operation': [p.to_dict()],
    }

    context = operation.OperationContext.from_data_inputs(identifier, data_inputs)

    mocker.patch.object(context, 'action')

    context.input_var_names = {p.name: ['pr']}

    result = cdat.PROCESS_FUNC_MAP[identifier](context, p, *inputs)

    v = 'pr' if output_name is None else output_name

    if isinstance(output, np.ndarray):
        assert np.array_equal(result[v].values, output)
    elif isinstance(output, tuple):
        print(getattr(result[v], output[0])().values, output[1])

        assert np.array_equal(getattr(result[v], output[0])().values, output[1])
    else:
        expected = test_data.standard(output)

        assert np.array_equal(result[v].values, expected.pr.values)


@pytest.mark.parametrize('domain,decode_times,expected', [
    (cwt.Domain(time=slice(0, 10)), True, (10, 64, 128)),
    (cwt.Domain(time=(1049.0, 1960.5)), False, (31, 64, 128)),
    (cwt.Domain(time=('1850', '1851')), True, (24, 64, 128)),
    (cwt.Domain(time=slice(0, 10), lat=(-45, 45)), True, (10, 32, 128)),
    pytest.param(cwt.Domain(time=slice(2000, 4000)), True, (2000, 64, 128), marks=pytest.mark.xfail),
])
def test_subset_input(mocker, domain, decode_times, expected):
    context = operation.OperationContext()

    process = cwt.Process('CDAT.subset')
    process.set_domain(domain)

    ds = xr.open_dataset(CMIP6_CLT, decode_times=decode_times)

    output = cdat.subset_input(context, process, ds)

    assert output.clt.shape == expected


@pytest.mark.myproxyclient
@pytest.mark.parametrize('url,var_name,chunks,exp_chunks', [
    (CMIP5_CLT, 'clt', {'time': 100}, [906, 1, 1]),
    pytest.param(CMIP5_CLT, 'pr', {'time': 100}, [906, 1, 1], marks=pytest.mark.xfail),
    pytest.param(CMIP5_CLT, 'clt', {'time': 1e20}, [906, 1, 1], marks=pytest.mark.xfail),
])
def test_build_dataset(mpc, url, var_name, chunks, exp_chunks):
    with cdat.chdir_temp() as tempdir:
        with open('cert.pem', 'w') as outfile:
            outfile.write(mpc)

        cdat.write_dodsrc('./cert.pem')

        ds = xr.open_dataset(url)

        ds_ = cdat.build_dataset(url, var_name, ds, chunks, mpc)

        assert ds.attrs == ds_.attrs
        assert ds.data_vars.keys() == ds_.data_vars.keys()

@pytest.mark.myproxyclient
@pytest.mark.parametrize('url,var_name,chunks,exp_chunks', [
    (CMIP5_CLT, 'clt', {'time': 100}, [906, 1, 1]),
    pytest.param(CMIP5_CLT, 'pr', {'time': 100}, [906, 1, 1], marks=pytest.mark.xfail),
    pytest.param(CMIP5_CLT, 'clt', {'time': 1e20}, [906, 1, 1], marks=pytest.mark.xfail),
])
def test_build_dataarray(mpc, url, var_name, chunks, exp_chunks):
    with cdat.chdir_temp() as tempdir:
        with open('cert.pem', 'w') as outfile:
            outfile.write(mpc)

        cdat.write_dodsrc('./cert.pem')

        ds = xr.open_dataset(url)

        da = cdat.build_dataarray(url, var_name, ds[var_name], chunks, mpc)

        assert da.shape == ds[var_name].shape
        assert da.dtype == ds[var_name].dtype
        assert da.name == ds[var_name].name
        assert da.attrs == ds[var_name].attrs
        assert [len(x) for x in da.chunks] == exp_chunks


@pytest.mark.myproxyclient
@pytest.mark.parametrize('url,var_name,chunks,exp_chunks', [
    (CMIP5_CLT, 'clt', {'time': 100}, [906, 1, 1]),
    pytest.param(CMIP5_CLT, 'pr', {'time': 100}, [906, 1, 1], marks=pytest.mark.xfail),
    pytest.param(CMIP5_CLT, 'clt', {'time': 1e20}, [906, 1, 1], marks=pytest.mark.xfail),
])
def test_build_dask_array(mpc, url, var_name, chunks, exp_chunks):
    with cdat.chdir_temp() as tempdir:
        with open('cert.pem', 'w') as outfile:
            outfile.write(mpc)

        cdat.write_dodsrc('./cert.pem')

        ds = xr.open_dataset(url)

        dataarray = ds[var_name]

        da = cdat.build_dask_array(url, var_name, dataarray, chunks, mpc)

        assert da.shape == ds[var_name].shape
        assert da.dtype == ds[var_name].dtype
        assert [len(x) for x in da.chunks] == exp_chunks


@pytest.mark.myproxyclient
@pytest.mark.parametrize('url,var_name,chunk,expected_shape', [
    (CMIP6_CLT, 'clt', None, (10, 64, 128)),
    pytest.param('{!s}c'.format(CMIP6_CLT), 'clt', None, (10, 64, 128), marks=pytest.mark.xfail),
    pytest.param(CMIP6_CLT, 'pr', (10, 64, 128), None, marks=pytest.mark.xfail),
    pytest.param(CMIP6_CLT, 'clt', (10, 64, 128), {'time': slice(1e6, 1e6+10)}, marks=pytest.mark.xfail),
])
def test_get_protected_data(mpc, url, var_name, chunk, expected_shape):
    if chunk is None:
        chunk = {'time': slice(100, 110)}

    data = cdat.get_protected_data(url, var_name, mpc, **chunk)

    assert var_name in data.name
    assert data.shape == expected_shape
    assert data.sum(dim=['time', 'lat', 'lon']).values == 5585352.5


@pytest.mark.parametrize('shape,index,size,expected', [
    ([10, 12, 14], 1, 32, [10, 32, 14]),
    pytest.param([], 1, 32, [], marks=pytest.mark.xfail),
])
def test_update_shape(shape, index, size, expected):
    output = cdat.update_shape(shape, index, size)

    assert output == expected


def test_write_dodsrc(mocker):
    m = mocker.patch('compute_tasks.cdat.open', mocker.mock_open())

    cdat.write_dodsrc('cert.pem')

    m.assert_called_with(os.path.join(os.getcwd(), '.dodsrc'), 'w')

    e = m.return_value.__enter__.return_value

    e.write.assert_any_call('HTTP.COOKIEJAR=.cookies\n')
    e.write.assert_any_call('HTTP.SSL.CERTIFICATE=cert.pem\n')
    e.write.assert_any_call('HTTP.SSL.KEY=cert.pem\n')
    e.write.assert_any_call('HTTP.SSL.CAPATH=cert.pem\n')
    e.write.assert_any_call('HTTP.SSL.VALIDATE=false\n')

def test_chdir_temp(mocker):
    mocker.patch.object(os, 'chdir')
    mocker.patch.object(os, 'getcwd', return_value='/test')

    with cdat.chdir_temp() as tempdir:
        pass

    os.chdir.assert_any_call(tempdir)
    os.chdir.assert_any_call('/test')

@pytest.mark.myproxyclient
@pytest.mark.parametrize('url, expected_size', [
    (CMIP5_CLT, (90520, 64, 128)),
    (CMIP6_CLT, (1812, 64, 128)),
])
def test_open_dataset(mocker, url, expected_size):
    context = operation.OperationContext()

    m = MyProxyClient(hostname=os.environ['MPC_HOST'])

    cert = m.logon(os.environ['MPC_USERNAME'], os.environ['MPC_PASSWORD'], bootstrap=True)

    mocker.patch.object(context, 'user_cert', return_value=''.join([x.decode() for x in cert]))

    ds = cdat.open_dataset(context, url, 'clt', chunks={'time': 100})

    assert ds
    assert ds.clt.shape == expected_size
    assert 'lat_bnds' in ds
    assert 'lon_bnds' in ds
    assert 'time_bnds' in ds


def test_execute_delayed_with_client(mocker):
    mocker.patch.object(dask, 'compute')
    mocker.patch.object(cdat, 'DaskJobTracker')

    context = mocker.MagicMock()

    client = mocker.MagicMock()  # noqa: F811

    futures = []

    cdat.execute_delayed(context, futures, client)

    dask.compute.assert_not_called()

    client.compute.assert_called_with(futures)

    cdat.DaskJobTracker.assert_called_with(context, client.compute.return_value)


def test_execute_delayed(mocker):
    mocker.patch.object(dask, 'compute')

    context = mocker.MagicMock()

    futures = []

    cdat.execute_delayed(context, futures)

    dask.compute.assert_called_with(futures)


def test_check_access_exception(esgf_data):
    with pytest.raises(WPSError):
        cdat.check_access('https://esgf-node.llnl.gov/sjdlasjdla')


def test_check_access_request_exception(esgf_data):
    with pytest.raises(WPSError):
        cdat.check_access('https://ajsdklajskdja')


@pytest.mark.parametrize('status_code', [
    (401),
    (403),
])
def test_check_access_protected(mocker, esgf_data, status_code):
    requests = mocker.patch.object(cdat, 'requests')

    requests.get.return_value.status_code = status_code

    assert not cdat.check_access(esgf_data.data['tas-opendap-cmip5']['files'][0])


def test_check_access(esgf_data):
    assert cdat.check_access(esgf_data.data['tas-opendap']['files'][0])


def test_clean_output(mocker, esgf_data):
    ds = esgf_data.to_xarray('tas-opendap')

    cdat.clean_output(ds)

    assert 'missing_value' not in ds.tas.encoding


@pytest.mark.dask
def test_dask_job_tracker(mocker, client):  # noqa: F811
    context = mocker.MagicMock()

    data = da.random.random((100, 100), chunks=(10, 10))

    fut = client.compute(data)

    cdat.DaskJobTracker(context, fut)

    assert context.message.call_count > 0


def test_gather_workflow_outputs_bad_coord(mocker):
    context = mocker.MagicMock()

    subset = cwt.Process(identifier='CDAT.subset')
    subset_delayed = mocker.MagicMock()
    subset_delayed.to_netcdf.side_effect = ValueError

    interm = {
        subset.name: subset_delayed,
    }

    with pytest.raises(WPSError):
        cdat.gather_workflow_outputs(context, interm, [subset])


def test_gather_workflow_outputs_missing_interm(mocker):
    context = mocker.MagicMock()

    subset = cwt.Process(identifier='CDAT.subset')
    subset_delayed = mocker.MagicMock()

    max = cwt.Process(identifier='CDAT.max')

    interm = {
        subset.name: subset_delayed,
    }

    with pytest.raises(WPSError):
        cdat.gather_workflow_outputs(context, interm, [subset, max])


def test_gather_workflow_outputs(mocker):
    context = mocker.MagicMock()

    subset = cwt.Process(identifier='CDAT.subset')
    subset_delayed = mocker.MagicMock()

    max = cwt.Process(identifier='CDAT.max')
    max_delayed = mocker.MagicMock()

    interm = {
        subset.name: subset_delayed,
        max.name: max_delayed,
    }

    delayed = cdat.gather_workflow_outputs(context, interm, [subset, max])

    assert len(delayed) == 2
    assert len(interm) == 0


@pytest.mark.myproxyclient
@pytest.mark.parametrize('identifier,inputs,domain,expected,expected_type', [
    ('CDAT.subset', [CMIP6_AGG1], None, 1, cftime.DatetimeNoLeap),
    ('CDAT.subtract', [CMIP6_AGG1, CMIP6_AGG2], None, 2, cftime.DatetimeNoLeap),
    ('CDAT.subset', [CMIP6_AGG1], cwt.Domain(time=(10, 200)), 1, np.float64),
    ('CDAT.aggregate', [CMIP5_AGG1, CMIP5_AGG2], None, 1, cftime.DatetimeNoLeap),
])
def test_gather_inputs(mocker, mpc, identifier, inputs, domain, expected, expected_type):
    process = cwt.Process(identifier)
    process.add_inputs(*[cwt.Variable(x, 'tas') for x in inputs])
    process.set_domain(domain)

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')
    mocker.patch.object(context, 'user_cert', return_value=mpc)

    data = cdat.gather_inputs(context, process)

    assert len(data) == expected
    assert isinstance(data[0].time.values[0], expected_type)


SUBSET = cwt.Process(identifier='CDAT.subset', name='subset')
SUBSET.add_inputs(cwt.Variable(CMIP6_CLT, 'clt'))

MAX = cwt.Process(identifier='CDAT.max', name='max')
MAX.add_inputs(SUBSET)
MAX.add_parameters(axes='time')

@pytest.mark.parametrize('processes,expected', [
    ([SUBSET], 1),
    ([SUBSET, MAX], 2)
])
def test_build_workflow(mocker, processes, expected):
    context = operation.OperationContext()
    context._sorted = [x.name for x in processes]
    context._operation = dict([(x.name, x) for x in processes])
    context.input_var_names = {
        'subset': ['clt'],
        'max': ['clt'],
    }

    mocker.patch.object(context, 'action')

    interm = cdat.build_workflow(context)

    assert len(interm) == expected


def test_workflow_error(mocker):
    mocker.patch.object(cdat, 'gather_workflow_outputs')
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'build_workflow')
    mocker.patch.object(cdat, 'DaskJobTracker')
    mocker.patch.object(cdat, 'execute_delayed')

    context = mocker.MagicMock()
    context.extra = {
        'DASK_SCHEDULER': 'dask-scheduler',
    }

    cdat.execute_delayed.side_effect = WPSError

    with pytest.raises(WPSError):
        cdat.workflow(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_called_with('dask-scheduler')

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.Client.return_value.compute.assert_not_called()


def test_workflow_execute_error(mocker):
    mocker.patch.object(cdat, 'gather_workflow_outputs')
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'build_workflow')
    mocker.patch.object(cdat, 'DaskJobTracker')

    context = mocker.MagicMock()
    context.extra = {
        'DASK_SCHEDULER': 'dask-scheduler',
    }

    cdat.Client.return_value.compute.side_effect = Exception()

    with pytest.raises(WPSError):
        cdat.workflow(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_called_with('dask-scheduler')

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.Client.return_value.compute.assert_called()


def test_workflow_no_client(mocker):
    mocker.patch.object(cdat, 'gather_workflow_outputs')
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'build_workflow')
    mocker.patch.object(cdat, 'DaskJobTracker')

    context = mocker.MagicMock()
    context.extra = {}

    cdat.workflow(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_not_called()

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.Client.return_value.compute.assert_not_called()


def test_workflow(mocker):
    mocker.patch.object(cdat, 'gather_workflow_outputs')
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'build_workflow')
    mocker.patch.object(cdat, 'DaskJobTracker')

    context = mocker.MagicMock()
    context.extra = {
        'DASK_SCHEDULER': 'dask-scheduler',
    }

    cdat.workflow(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_called_with('dask-scheduler')

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.Client.return_value.compute.assert_called()


def test_process_wrapper(mocker):
    mocker.patch.object(cdat, 'workflow')

    context = mocker.MagicMock()

    self = mocker.MagicMock()

    cdat.process_wrapper(self, context)

    cdat.workflow.assert_called_with(context)


SINGLE_ABS = """Test description.

Accepts exactly 1 input.
"""

MIN_MAX_ABS = """Test description.

Accepts 2 to 4 inputs.
"""

INF_ABS = """Test description.

Accepts a minimum of 1 inputs.
"""

MULTI_PARAM_ABS = """Test description.

Accepts exactly 1 input.

Parameters:
    axes (str, Required): A list of axes to reduce dimensionality over. Separate multiple values with "|" e.g. time|lat.
    const (float, Optional): A float value that will be applied element-wise.
    bins (str, Optional): A list of bins. Separate values with "|" e.g. 0|10|20, this would create 2 bins (0-10), (10, 20).
"""

@pytest.mark.parametrize('identifier,description,params,expected', [
    ('CDAT.subset', 'Test description.', {}, SINGLE_ABS),
    ('CDAT.subset', 'Test description.', {'min': 2, 'max': 4}, MIN_MAX_ABS),
    ('CDAT.subset', 'Test description.', {'max': float('inf')}, INF_ABS),
    ('CDAT.subset', 'Test description.', {'const': cdat.parameter(float), 'axes': cdat.parameter(str, True), 'bins': cdat.parameter(str)}, MULTI_PARAM_ABS),
])
def test_render_abstract(identifier, description, params, expected):
    cdat.render_abstract(identifier, description, **params)

    assert identifier in cdat.ABSTRACT
    assert cdat.ABSTRACT[identifier] == expected

    min = params.pop('min', 1)
    max = params.pop('max', 1)

    assert identifier in cdat.VALIDATION

    v = cdat.VALIDATION[identifier]

    assert 'min' in v and v['min'] == min
    assert 'max' in v and v['max'] == max

    for x, y in params.items():
        assert x in v['params']

        for xx, yy in y.items():
            assert xx in v['params'][x]
            assert yy == v['params'][x][xx]


def test_discover_processes(mocker):
    mocker.spy(builtins, 'setattr')
    mocker.patch.object(base, 'cwt_shared_task')
    mocker.patch.object(base, 'register_process')

    cdat.discover_processes()

    base.cwt_shared_task.assert_called()
    base.cwt_shared_task.return_value.assert_called()

    base.register_process.assert_any_call('CDAT', 'subset', abstract=cdat.ABSTRACT['CDAT.subset'])

    builtins.setattr.assert_any_call(cdat, 'subset_func', base.register_process.return_value.return_value)
