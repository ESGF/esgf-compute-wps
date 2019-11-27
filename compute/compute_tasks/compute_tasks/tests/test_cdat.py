import builtins
import json
import re
import os

import cdms2
import cwt
import dask
import pytest
import dask.array as da
import xarray as xr
from distributed.utils_test import (  # noqa: F401
    client,
    loop,
    cluster_fixture,
)
from jinja2 import Environment, BaseLoader
from myproxy.client import MyProxyClient

from compute_tasks import base
from compute_tasks import cdat
from compute_tasks import WPSError
from compute_tasks.context import operation


@pytest.mark.parametrize('identifier,expected_shape,params,multiple', [
    ('CDAT.abs', (192, 288, 2, 3650), {}, False),
    ('CDAT.max', (192, 288, 2), {'axes': 'time'}, False),
    ('CDAT.max', (2, 3650), {'axes': ['lat', 'lon']}, False),
    ('CDAT.add', (192, 288, 2, 3650), {}, True),
])
@pytest.mark.data
def test_processing(esgf_data, identifier, expected_shape, params, multiple):
    v = cwt.Variable(esgf_data.to_local_path('tas'), 'tas')

    inputs = [v]

    if multiple:
        inputs.append(cwt.Variable(esgf_data.to_local_path('tas', file_index=1), 'tas'))

    p = cwt.Process(identifier)
    p.add_inputs(*inputs)
    p.add_parameters(**params)

    data_inputs = {
        'variable': json.dumps([x.to_dict() for x in inputs]),
        'domain': json.dumps([]),
        'operation': json.dumps([p.to_dict()]),
    }

    context = operation.OperationContext.from_data_inputs(identifier, data_inputs)

    ds_inputs = [xr.open_dataset(x.uri, chunks={'time': 100}) for x in inputs]

    output = cdat.PROCESS_FUNC_MAP[identifier](context, p, *ds_inputs)

    assert tuple(output.dims.values()) == expected_shape


@pytest.mark.data
@pytest.mark.myproxyclient
def test_protected_data(mocker, esgf_data):
    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/',
    })

    self = mocker.MagicMock()

    v1 = cwt.Variable(esgf_data.data['tas-opendap-cmip5']['files'][0], 'tas')

    subset = cwt.Process('CDAT.subset')
    subset.set_domain(cwt.Domain(time=(50, 100)))
    subset.add_inputs(v1)

    data_inputs = {
        'variable': json.dumps([v1.to_dict()]),
        'domain': json.dumps([subset.domain.to_dict()]),
        'operation': json.dumps([subset.to_dict()]),
    }

    ctx = operation.OperationContext.from_data_inputs('CDAT.subset', data_inputs)
    ctx.user = 0
    ctx.job = 0

    m = MyProxyClient(hostname=os.environ['MPC_HOST'])

    cert = m.logon(os.environ['MPC_USERNAME'], os.environ['MPC_PASSWORD'], bootstrap=True)

    mocker.patch.object(ctx, 'user_cert', return_value=''.join([x.decode() for x in cert]))

    mocker.patch.object(ctx, 'message')
    mocker.patch.object(ctx, 'action')

    new_ctx = cdat.process_wrapper(self, ctx)

    assert new_ctx

    with cdms2.open(new_ctx.output[0].uri) as infile:
        var_name = new_ctx.output[0].var_name

        assert infile[var_name].shape == (50, 96, 192)


@pytest.mark.data
def test_subset(mocker, esgf_data, client):  # noqa: F811
    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/',
    })

    self = mocker.MagicMock()

    v1 = cwt.Variable(esgf_data.data['tas-opendap']['files'][0], 'tas')

    subset = cwt.Process('CDAT.subset')
    subset.set_domain(cwt.Domain(time=(674885.0, 674911.0)))
    subset.add_inputs(v1)

    data_inputs = {
        'variable': json.dumps([v1.to_dict()]),
        'domain': json.dumps([subset.domain.to_dict()]),
        'operation': json.dumps([subset.to_dict()]),
    }

    ctx = operation.OperationContext.from_data_inputs('CDAT.subset', data_inputs)
    ctx.extra['DASK_SCHEDULER'] = client.scheduler.address
    ctx.user = 0
    ctx.job = 0

    print(ctx.gparameters)

    mocker.patch.object(ctx, 'message')
    mocker.patch.object(ctx, 'action')

    new_ctx = cdat.process_wrapper(self, ctx)

    assert new_ctx

    with cdms2.open(new_ctx.output[0].uri) as infile:
        var_name = new_ctx.output[0].var_name

        assert infile[var_name].shape == (27, 192, 288)


def test_subset_input_value_spatial(mocker, esgf_data):
    context = operation.OperationContext()
    context._variable = {'tas': cwt.Variable('', 'tas')}

    process = cwt.Process('CDAT.subset')
    process.set_domain(cwt.Domain(lat=(-45, 45)))

    input = esgf_data.to_xarray('tas-opendap')

    new_input = cdat.subset_input(context, process, input)

    assert list(new_input.dims.values()) == [96, 288, 2, 3650]


def test_subset_input_value_timestamps(mocker, esgf_data):
    context = operation.OperationContext()
    context._variable = {'tas': cwt.Variable('', 'tas')}

    process = cwt.Process('CDAT.subset')
    process.set_domain(cwt.Domain(time=('1850', '1851')))

    input = esgf_data.to_xarray('tas-opendap')

    new_input = cdat.subset_input(context, process, input)

    assert list(new_input.dims.values()) == [192, 288, 2, 730]


def test_subset_input_value_time(mocker, esgf_data):
    context = operation.OperationContext()
    context._variable = {'tas': cwt.Variable('', 'tas')}

    process = cwt.Process('CDAT.subset')
    process.set_domain(cwt.Domain(time=(674885., 674985.)))

    input = esgf_data.to_xarray('tas-opendap')

    new_input = cdat.subset_input(context, process, input)

    assert list(new_input.dims.values()) == [192, 288, 2, 101]


def test_subset_input_indices(mocker, esgf_data):
    context = operation.OperationContext()

    process = cwt.Process('CDAT.subset')
    process.set_domain(cwt.Domain(time=slice(0, 10)))

    input = esgf_data.to_xarray('tas-opendap')

    new_input = cdat.subset_input(context, process, input)

    assert list(new_input.dims.values()) == [192, 288, 2, 10]


def test_filter_protected_exception(mocker):
    ctx = mocker.MagicMock()
    ctx.user_cert.return_value = 'cert data'

    mocker.patch.object(cdat, 'check_access', side_effect=[False, False])

    args = [
        ctx,
        ['/file1.nc', '/file2.nc'],
    ]

    with pytest.raises(WPSError):
        cdat.filter_protected(*args)


def test_filter_protected(mocker):
    ctx = mocker.MagicMock()
    ctx.user_cert.return_value = 'cert data'

    mocker.patch.object(cdat, 'check_access', side_effect=[False, True, True])

    args = [
        ctx,
        ['/file1.nc', '/file2.nc'],
    ]

    unprotected, protected, cert_tempfile = cdat.filter_protected(*args)

    assert len(unprotected) == 1
    assert len(protected) == 1
    assert cert_tempfile is not None


@pytest.mark.data
@pytest.mark.myproxyclient
def test_localize_protected(mocker, esgf_data, client):
    output_path = '/cache/501c6894f7a3b053e694cff02ec081fb69f4f736026bad72e9b7faff161589d8.nc'

    if os.path.exists(output_path):
        os.remove(output_path)

    mocker.patch.dict(os.environ, {
        'DATA_PATH': '/',
    })

    context = operation.OperationContext()

    m = MyProxyClient(hostname=os.environ['MPC_HOST'])

    cert = m.logon(os.environ['MPC_USERNAME'], os.environ['MPC_PASSWORD'], bootstrap=True)

    mocker.patch.object(context, 'user_cert', return_value=''.join([x.decode() for x in cert]))

    mocker.spy(os.path, 'exists')

    output = cdat.localize_protected(context, esgf_data.data['tas-opendap-cmip5']['files'][:1], None)

    assert len(output) == 1
    assert output[0] == output_path
    assert not os.path.exists.return_value

    output = cdat.localize_protected(context, esgf_data.data['tas-opendap-cmip5']['files'][:1], None)

    assert len(output) == 1
    assert output[0] == output_path
    assert os.path.exists.return_value


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


def test_get_user_cert(mocker):
    context = mocker.MagicMock()
    context.user_cert.return_value = 'client cert data'

    tf = cdat.get_user_cert(context)

    assert tf


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


def test_clean_variable_encoding(mocker, esgf_data):
    ds = esgf_data.to_xarray('tas-opendap')

    cdat.clean_variable_encoding(ds)

    assert 'missing_value' not in ds.tas.encoding


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


def test_gather_inputs_aggregate(mocker, esgf_data):
    process = cwt.Process('CDAT.aggregate')
    process.add_inputs(*[cwt.Variable(x, 'tas') for x in esgf_data.data['tas-opendap']['files']])

    context = mocker.MagicMock()

    data = cdat.gather_inputs(context, process)

    assert len(data) == 1


def test_gather_inputs_exception(mocker, esgf_data):
    tempfile_mock = mocker.MagicMock()

    mocker.patch.object(cdat, 'filter_protected', return_value=([], esgf_data.data['tas-opendap']['files'],
                        tempfile_mock))
    mocker.patch.object(cdat, 'localize_protected', return_value=esgf_data.data['tas-opendap']['files'])

    files = esgf_data.data['tas-opendap-cmip5']['files']

    process = cwt.Process('CDAT.subset')
    process.add_inputs(*[cwt.Variable(x, 'tas') for x in files])

    context = mocker.MagicMock()
    context.user_cert.return_value = 'cert data'

    data = cdat.gather_inputs(context, process)

    assert len(data) == 2
    cdat.localize_protected.assert_called()


def test_gather_inputs(mocker, esgf_data):
    process = cwt.Process('CDAT.subset')
    process.add_inputs(*[cwt.Variable(x, 'tas') for x in esgf_data.data['tas-opendap']['files']])

    context = mocker.MagicMock()

    data = cdat.gather_inputs(context, process)

    assert len(data) == 2


def test_build_workflow_missing_interm(mocker):
    process_func = mocker.MagicMock()
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
        'CDAT.subset': process_func,
    }, True)

    subset = cwt.Process(identifier='CDAT.subset')

    mocker.patch.object(subset, 'copy')

    subset2 = cwt.Process(identifier='CDAT.subset')

    max = cwt.Process(identifier='CDAT.max')
    max.add_inputs(subset, subset2)

    context = mocker.MagicMock()
    context.topo_sort.return_value = [subset, max]

    with pytest.raises(WPSError):
        cdat.build_workflow(context)


def test_build_workflow_use_interm(mocker):
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
        'CDAT.subset': mocker.MagicMock(),
        'CDAT.max': mocker.MagicMock(),
    }, True)

    subset = cwt.Process(identifier='CDAT.subset')

    mocker.patch.object(subset, 'copy')

    max = cwt.Process(identifier='CDAT.max')
    max.add_inputs(subset)

    context = mocker.MagicMock()
    context.topo_sort.return_value = [subset, max]

    interm = cdat.build_workflow(context)

    cdat.gather_inputs.assert_called_with(context, subset)

    assert len(interm) == 2
    assert subset.name in interm
    assert max.name in interm


def test_build_workflow_process_func(mocker):
    process_func = mocker.MagicMock()
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
        'CDAT.subset': process_func,
    }, True)

    subset = cwt.Process(identifier='CDAT.subset')

    context = mocker.MagicMock()
    context.topo_sort.return_value = [subset]

    interm = cdat.build_workflow(context)

    cdat.gather_inputs.assert_called_with(context, subset)

    assert len(interm) == 1
    assert subset.name in interm


def test_build_workflow(mocker):
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
        'CDAT.subset': mocker.MagicMock(),
    }, True)

    subset = cwt.Process(identifier='CDAT.subset')

    context = mocker.MagicMock()
    context.topo_sort.return_value = [subset]

    interm = cdat.build_workflow(context)

    cdat.gather_inputs.assert_called_with(context, subset)

    assert len(interm) == 1
    assert subset.name in interm


@pytest.mark.skip(reason='Unconfigurable exponential timeout')
def test_workflow_func_dask_cluster_error(mocker):
    mocker.patch.object(cdat, 'gather_workflow_outputs')
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'build_workflow')
    mocker.patch.object(cdat, 'DaskJobTracker')

    context = mocker.MagicMock()
    context.extra = {
        'DASK_SCHEDULER': 'dask-scheduler',
    }

    cdat.Client.side_effect = OSError()

    cdat.workflow_func(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_called_with('dask-scheduler.default.svc:8786')


def test_workflow_func_error(mocker):
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
        cdat.workflow_func(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_called_with('dask-scheduler')

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.Client.return_value.compute.assert_not_called()


def test_workflow_func_execute_error(mocker):
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
        cdat.workflow_func(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_called_with('dask-scheduler')

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.Client.return_value.compute.assert_called()


def test_workflow_func_no_client(mocker):
    mocker.patch.object(cdat, 'gather_workflow_outputs')
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'build_workflow')
    mocker.patch.object(cdat, 'DaskJobTracker')

    context = mocker.MagicMock()
    context.extra = {}

    cdat.workflow_func(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_not_called()

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.Client.return_value.compute.assert_not_called()


def test_workflow_func(mocker):
    mocker.patch.object(cdat, 'gather_workflow_outputs')
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'build_workflow')
    mocker.patch.object(cdat, 'DaskJobTracker')

    context = mocker.MagicMock()
    context.extra = {
        'DASK_SCHEDULER': 'dask-scheduler',
    }

    cdat.workflow_func(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_called_with('dask-scheduler')

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.Client.return_value.compute.assert_called()


def test_process_wrapper(mocker):
    mocker.patch.object(cdat, 'workflow_func')

    context = mocker.MagicMock()

    self = mocker.MagicMock()

    cdat.process_wrapper(self, context)

    cdat.workflow_func.assert_called_with(context)


def test_render_abstract_no_max():
    d = 'This a test description.'

    text = cdat.render_abstract(d, max_inputs=None)

    assert d in text
    assert re.search('.*minimum of 1.*', text) is not None


def test_render_abstract_more_inputs():
    d = 'This a test description.'

    text = cdat.render_abstract(d, max_inputs=2)

    assert d in text
    assert re.search('.*minimum of 1.*', text) is not None
    assert re.search('.*maximum of 2.*', text) is not None


def test_render_abstract():
    d = 'This a test description.'

    text = cdat.render_abstract(d)

    assert d in text
    assert 'A single input is required.' in text


def test_discover_processes(mocker):
    mocker.spy(builtins, 'setattr')
    mocker.patch.object(base, 'cwt_shared_task')
    mocker.patch.object(base, 'register_process')

    cdat.discover_processes()

    base.cwt_shared_task.assert_called()
    base.cwt_shared_task.return_value.assert_called()

    base.register_process.assert_any_call('CDAT', 'subset', abstract=cdat.ABSTRACT_MAP['CDAT.subset'])

    builtins.setattr.assert_any_call(cdat, 'subset_func', base.register_process.return_value.return_value)
