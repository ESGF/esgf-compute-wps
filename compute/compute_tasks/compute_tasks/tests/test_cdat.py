import builtins
import json
import re
import os
import tempfile
import sys

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

np.set_printoptions(threshold=sys.maxsize)

class TestDataGenerator(object):
    def generate(self, type=None, value=None, name=None, time_start=None):
        type = type or 'standard'
        name = name or 'pr'
        time_start = time_start or '1990-01-01'
        value = value or 5

        if type == 'standard':
            data = np.full((10, 90, 180), value)
        elif type == 'random':
            np.random.seed(0)
            data = np.random.normal(size=(10, 90, 180))
        else:
            raise Exception('Unknown type')

        data_vars = {
            name: (('time', 'lat', 'lon'), data),
        }

        coords = {
            'time': pd.date_range(time_start, periods=10),
            'lat': (['lat'], np.arange(-90, 90, 2)),
            'lon': (['lon'], np.arange(0, 360, 2)),
        }

        return xr.Dataset(data_vars, coords)

    def groupby_bins(self):
        ds = self.generate('random')

        return ds.groupby_bins('pr', bins=np.arange(0.0, 1.0, 0.1))


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

    v1 = test_data.generate('random')

    p = cwt.Process(identifier)

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')

    with pytest.raises(WPSError):
        base.get_process(identifier)._process_func(context, p, *[v1], variable=['prw'], bins=['abcd'])


def test_groupby_bins_missing_variable(test_data, mocker):
    identifier = 'CDAT.groupby_bins'

    v1 = test_data.generate('random')

    p = cwt.Process(identifier)

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')

    with pytest.raises(WPSError):
        base.get_process(identifier)._process_func(context, p, *[v1], variable=['prw'], bins=np.arange(0.0, 1.0, 0.1))


def test_groupby_bins(test_data, mocker):
    identifier = 'CDAT.groupby_bins'

    v1 = test_data.generate('random')

    p = cwt.Process(identifier)

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')

    output = base.get_process(identifier)._process_func(context, p, *[v1], variable=['pr'], bins=np.arange(0.0, 1.0, 0.1))

    assert len(output) == 9


@pytest.mark.parametrize('cond', [
    'lat>-45',
    'lat>45',
    'lon>360',
    'lon>=360',
    'lon<180',
    'lon<=180',
    'lon==180',
    'lon!=180',
    'pr isnull',
    'pr notnull',
    'prisnull',
])
def test_where(test_data, cond, mocker):
    identifier = 'CDAT.where'

    v1 = test_data.generate()

    p = cwt.Process(identifier)

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')

    result = base.get_process(identifier)._process_func(context, p, *[v1], variable=None, cond=cond, other=None)

    assert 'pr' in result


def test_where_other(test_data, mocker):
    identifier = 'CDAT.where'

    v1 = test_data.generate('random')

    p = cwt.Process(identifier)

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')

    result = base.get_process(identifier)._process_func(context, p, *[v1], variable=None, cond='pr<0.5', other=1e20)

    assert 'pr' in result

    print(np.sum(result.pr).values)

    assert np.sum(result.pr).values == np.array(5.0156e+24)


@pytest.mark.parametrize('cond', [
    'lon>>>>180',
    'lon>>180',
    None,
    'tas>-45',
])
def test_where_error(test_data, cond, mocker):
    identifier = 'CDAT.where'

    v1 = test_data.generate()

    p = cwt.Process(identifier)

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')

    with pytest.raises(WPSError):
        base.get_process(identifier)._process_func(context, p, *[v1], variable=None, cond=cond, other=None)


def test_merge(test_data, mocker):
    identifier = 'CDAT.merge'

    v1 = test_data.generate(name='pr')
    v2 = test_data.generate(name='prw')

    inputs = [v1, v2]

    p = cwt.Process(identifier)

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')

    result = base.get_process(identifier)._process_func(context, p, *inputs, compat=None)

    assert 'pr' in result
    assert 'prw' in result

def params(**kwargs):
    p = {
        'variable': None,
        'fillna': None,
        'rename': None
    }

    p.update(kwargs)

    return p

POS_5 = {'value': 5}
POS_2 = {'value': 2}
NEG_5 = {'value': -5}
RAND = {'type': 'random'}

@pytest.mark.parametrize('identifier,v1,v2,output,output_var,extra', [
    ('CDAT.abs', NEG_5, None, np.array(810000), 'pr', params()),
    ('CDAT.abs', NEG_5, None, np.array(810000), 'pr', params(variable=['pr'])),
    ('CDAT.abs', NEG_5, None, np.array(810000), 'pr', params(fillna=1e20)),
    ('CDAT.abs', NEG_5, None, np.array(810000), 'pr_test', params(rename=['pr', 'pr_test'])),
    ('CDAT.add', POS_5, POS_2, np.array(1134000), 'pr', params(const=None)),
    ('CDAT.add', POS_5, None, np.array(1458000), 'pr', params(const=4)),
    ('CDAT.add', POS_5, None, np.array(1458000), 'pr', params(const=4, variable=['pr'])),
    ('CDAT.add', POS_5, None, np.array(1458000), 'pr', params(const=4, fillna=1e20)),
    ('CDAT.add', POS_5, None, np.array(1458000), 'pr_test', params(const=4, rename=['pr', 'pr_test'])),
    ('CDAT.divide', POS_5, POS_2, np.array(405000.), 'pr', params(const=None)),
    ('CDAT.divide', POS_5, POS_2, np.array(405000.), 'pr', params(const=None, variable=['pr', 'pr'])),
    ('CDAT.divide', POS_5, None, np.array(162000.), 'pr', params(const=5)),
    ('CDAT.exp', POS_5, None, np.array(24042931.77461741), 'pr', params()),
    ('CDAT.filter_map', RAND, None, np.array(57172.20906099566), 'pr', params(cond='pr>0.5', func='sum', other=None)),
    ('CDAT.filter_map', RAND, None, np.array(1.11844e+25), 'pr', params(cond='pr>0.5', func='sum', other=1e20)),
    ('CDAT.filter_map', RAND, None, np.array(57172.20906099566), 'pr', params(cond='pr>0.5', func='sum', other=None, fillna=1e20)),
    ('CDAT.filter_map', RAND, None, np.array(57172.20906099566), 'pr_test', params(cond='pr>0.5', func='sum', other=None, rename=['pr', 'pr_test'])),
    ('CDAT.log', POS_5, None, np.array(260728.94181432418), 'pr', params()),
    ('CDAT.max', RAND, None, np.array(4.285855641221728), 'pr', params(axes=None)),
    ('CDAT.max', RAND, None, np.array(24966.70967411557), 'pr', params(axes=['time'])),
    ('CDAT.max', RAND, None, np.array(4.285855641221728), 'pr', params(axes=None, variable=['pr'])),
    ('CDAT.max', RAND, None, np.array(4.285855641221728), 'pr', params(axes=None, fillna=1e20)),
    ('CDAT.max', RAND, None, np.array(4.285855641221728), 'pr_test', params(axes=None, rename=['pr', 'pr_test'])),
    ('CDAT.mean', RAND, None, np.array(0.005161633523396578), 'pr', params(axes=None)),
    ('CDAT.min', RAND, None, np.array(-4.852117653180117), 'pr', params(axes=None)),
    ('CDAT.multiply', POS_5, None, np.array(4050000), 'pr', params(const=5)),
    ('CDAT.power', POS_5, None, np.array(4050000), 'pr', params(const=2)),
    ('CDAT.subtract', POS_5, None, np.array(486000), 'pr', params(const=2)),
    ('CDAT.sum', RAND, None, np.array(836.1846307902457), 'pr', params(axes=None)),
    ('CDAT.where', RAND, None, np.array(57172.20906099566), 'pr', params(cond='pr>0.5', other=None)),
    ('CDAT.where', RAND, None, np.array(1.11844e+25), 'pr', params(cond='pr>0.5', other=1e20)),
    ('CDAT.count', RAND, None, np.array(162000), 'pr', params()),
    ('CDAT.std', RAND, None, np.array(0.9975010966537913), 'pr', params(axes=None)),
    ('CDAT.var', RAND, None, np.array(0.9950084378255163), 'pr', params(axes=None)),
    ('CDAT.sqrt', RAND, None, np.array(66860.64544173385), 'pr', params()),
])
def test_processing(mocker, test_data, identifier, v1, v2, output, output_var, extra):
    data = [test_data.generate(**v1)]

    if v2 is not None:
        data.append(test_data.generate(**v2))

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')

    p = cwt.Process(identifier)

    result = base.get_process(identifier)._process_func(context, p, *data, **extra)

    assert output_var in result

    print(np.sum(result[output_var]).values)

    assert np.sum(result[output_var]) == output

@pytest.mark.parametrize('identifier,v1,v2,output,output_var,extra', [
    ('CDAT.add', POS_5, None, None, 'pr', params(const=None)),
])

def test_processing_error(mocker, test_data, identifier, v1, v2, output, output_var, extra):
    data = [test_data.generate(**v1)]

    context = operation.OperationContext()

    mocker.patch.object(context, 'action')

    p = cwt.Process(identifier)

    with pytest.raises(WPSError):
        base.get_process(identifier)._process_func(context, p, *data, **extra)


@pytest.mark.parametrize('domain,decode_times,expected,params', [
    pytest.param(cwt.Domain(time=('1800-01', '1800-12')), True, (12, 64, 128), {}, marks=pytest.mark.xfail),
    pytest.param(cwt.Domain(time=(1718.5, 1718.5)), False, (64, 128), {}, marks=pytest.mark.xfail),
    (cwt.Domain(time=(1718.5, 1718.5)), False, (64, 128), {'method': 'nearest'}),
    (cwt.Domain(time=('1870-01', '1870-01')), True, (1, 64, 128), {}),
    (cwt.Domain(time=(1718, 1718)), False, (64, 128), {}),
    (cwt.Domain(time=slice(150, 150)), True, (64, 128), {}),
    (cwt.Domain(time=('1870-01', '1870-12')), True, (12, 64, 128), {}),
    (cwt.Domain(time=(1718, 2205.5)), False, (17, 64, 128), {}),
    (cwt.Domain(time=slice(150, 300, 3)), True, (50, 64, 128), {}),
    (None, True, (1812, 64, 128), {}),
])
def test_process_subset(mocker, domain, decode_times, expected, params):
    process = cwt.Process('CDAT.subset')
    process.set_domain(domain)
    process.add_parameters(**params)

    context = operation.OperationContext()
    context.input_var_names[process.name] = ['clt']

    mocker.patch.object(context, 'action')

    ds = xr.open_dataset(CMIP6_CLT, decode_times=decode_times)

    output = cdat.process_subset(context, process, ds, **params)

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

def test_render_abstract():
    for x in base.REGISTRY:
        p = base.get_process(x)

        assert p._render_abstract() is not None
