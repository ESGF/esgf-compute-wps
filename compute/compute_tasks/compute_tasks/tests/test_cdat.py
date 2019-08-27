import builtins

import cwt
import pytest
import dask.array as da
from distributed.utils_test import (  # noqa: F401
    client,
    loop,
    cluster_fixture,
)

from compute_tasks import base
from compute_tasks import cdat
from compute_tasks import managers
from compute_tasks import DaskClusterAccessError
from compute_tasks import WPSError


def test_dask_job_tracker(mocker, client):  # noqa: F811
    context = mocker.MagicMock()

    data = da.random.random((100, 100), chunks=(10, 10))

    fut = client.compute(data)

    cdat.DaskJobTracker(context, fut)

    assert context.message.call_count > 0


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

    subset_delayed.to_xarray.assert_called()
    subset_delayed.to_xarray.return_value.to_netcdf.assert_called()

    max_delayed.to_xarray.assert_called()
    max_delayed.to_xarray.return_value.to_netcdf.assert_called()


def test_gather_inputs_aggregate(mocker):
    input1 = mocker.MagicMock()
    input1.var_name = 'tas'

    input2 = mocker.MagicMock()
    input2.var_name = 'tas'

    inputs = [
        input1,
        input2
    ]

    fm = mocker.MagicMock()

    data = cdat.gather_inputs('CDAT.aggregate', fm, inputs)

    assert len(data) == 1


def test_gather_inputs(mocker):
    inputs = [
        mocker.MagicMock(),
        mocker.MagicMock(),
    ]

    fm = mocker.MagicMock()

    data = cdat.gather_inputs('CDAT.subset', fm, inputs)

    assert len(data) == 2


def test_build_workflow_missing_interm(mocker):
    process_func = mocker.MagicMock()
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.object(cdat, 'process')
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

    fm = mocker.MagicMock()

    with pytest.raises(WPSError):
        cdat.build_workflow(fm, context)


def test_build_workflow_use_interm(mocker):
    process_func = mocker.MagicMock()
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.object(cdat, 'process')
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
        'CDAT.subset': process_func,
    }, True)

    subset = cwt.Process(identifier='CDAT.subset')

    mocker.patch.object(subset, 'copy')

    max = cwt.Process(identifier='CDAT.max')
    max.add_inputs(subset)

    context = mocker.MagicMock()
    context.topo_sort.return_value = [subset, max]

    fm = mocker.MagicMock()

    interm = cdat.build_workflow(fm, context)

    cdat.gather_inputs.assert_called_with('CDAT.subset', fm, subset.inputs)

    cdat.process.assert_any_call(cdat.gather_inputs.return_value, context, process_func=process_func)
    cdat.process.assert_any_call([cdat.process.return_value.copy.return_value], context)

    assert len(interm) == 2
    assert subset.name in interm
    assert max.name in interm


def test_build_workflow_process_func(mocker):
    process_func = mocker.MagicMock()
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.object(cdat, 'process')
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
        'CDAT.subset': process_func,
    }, True)

    subset = cwt.Process(identifier='CDAT.subset')

    context = mocker.MagicMock()
    context.topo_sort.return_value = [subset]

    fm = mocker.MagicMock()

    interm = cdat.build_workflow(fm, context)

    cdat.gather_inputs.assert_called_with('CDAT.subset', fm, subset.inputs)

    cdat.process.assert_called_with(cdat.gather_inputs.return_value, context, process_func=process_func)

    assert len(interm) == 1
    assert subset.name in interm


def test_build_workflow(mocker):
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.object(cdat, 'process')
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
    }, True)

    subset = cwt.Process(identifier='CDAT.subset')

    context = mocker.MagicMock()
    context.topo_sort.return_value = [subset]

    fm = mocker.MagicMock()

    interm = cdat.build_workflow(fm, context)

    cdat.gather_inputs.assert_called_with('CDAT.subset', fm, subset.inputs)

    cdat.process.assert_called_with(cdat.gather_inputs.return_value, context)

    assert len(interm) == 1
    assert subset.name in interm


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

    with pytest.raises(DaskClusterAccessError):
        cdat.workflow_func(context)

    cdat.build_workflow.assert_called()

    cdat.Client.assert_called_with('dask-scheduler.default.svc:8786')


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

    cdat.Client.assert_called_with('dask-scheduler.default.svc:8786')

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.interm_ops.return_value)

    cdat.Client.return_value.compute.assert_called()


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

    cdat.Client.assert_called_with('dask-scheduler.default.svc:8786')

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.output_ops.return_value)

    cdat.gather_workflow_outputs.assert_any_call(context, cdat.build_workflow.return_value,
                                                 context.interm_ops.return_value)

    cdat.Client.return_value.compute.assert_called()


def test_process_process_func(mocker):
    input = mocker.MagicMock()
    input.subset.return_value = (mocker.MagicMock(), mocker.MagicMock())

    context = mocker.MagicMock()
    context.is_regrid = False

    process_func = mocker.MagicMock()

    output = cdat.process([input, input], context, process_func=process_func)

    input.subset.assert_called_with(context.domain)

    process_func.assert_called_with(context.operation, input, input)

    assert output == process_func.return_value


def test_process_regrid(mocker):
    mocker.patch.object(cdat, 'regrid')

    input = mocker.MagicMock()
    input.subset.return_value = (mocker.MagicMock(), mocker.MagicMock())

    context = mocker.MagicMock()
    context.is_regrid = True

    output = cdat.process([input, input], context)

    input.subset.assert_called_with(context.domain)

    cdat.regrid.assert_called_with(context.operation, output)

    assert output == input


def test_process_multiple_inputs(mocker):
    input = mocker.MagicMock()
    input.subset.return_value = (mocker.MagicMock(), mocker.MagicMock())
    # input.regrid_context.return_value = (mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock())

    context = mocker.MagicMock()
    context.is_regrid = False

    output = cdat.process([input, input], context)

    input.subset.assert_called_with(context.domain)

    assert output == input


def test_process(mocker):
    input = mocker.MagicMock()
    input.subset.return_value = (mocker.MagicMock(), mocker.MagicMock())

    context = mocker.MagicMock()
    context.is_regrid = False

    output = cdat.process([input], context)

    input.subset.assert_called_with(context.domain)

    assert output == input


def test_regrid_single_chunk(esgf_data):
    operation = cwt.Process(identifier='CDAT.regrid')

    gridder = cwt.Gridder(grid='gaussian~32')

    operation.gridder = gridder

    input = esgf_data.to_input_manager('tas', cwt.Domain(time=slice(1000, 1010)))

    cdat.regrid(operation, input)

    assert input.variable.shape == (10, 32, 64)

    assert input.axes['lat'].shape == (32, )

    assert input.axes['lon'].shape == (64, )

    assert input.vars['lat_bnds'].shape == (32, 2)

    assert input.vars['lon_bnds'].shape == (64, 2)

    # TODO verify actually data is correct


def test_regrid(esgf_data):
    operation = cwt.Process(identifier='CDAT.regrid')

    gridder = cwt.Gridder(grid='gaussian~32')

    operation.gridder = gridder

    input = esgf_data.to_input_manager('tas')

    cdat.regrid(operation, input)

    assert input.variable.shape == (3650, 32, 64)

    assert input.axes['lat'].shape == (32, )

    assert input.axes['lon'].shape == (64, )

    assert input.vars['lat_bnds'].shape == (32, 2)

    assert input.vars['lon_bnds'].shape == (64, 2)


def test_regrid_error(mocker):
    input1 = mocker.MagicMock()

    operation = cwt.Process(identifier='CDAT.regrid')

    with pytest.raises(cwt.errors.CWTError):
        cdat.regrid(operation, input1)


def test_process_input_axes(mocker):
    mocker.patch.object(cdat, 'process_single_input')

    process_func = mocker.MagicMock()

    operation = cwt.Process(identifier='CDAT.subset')

    operation.add_parameters(axes=['time', 'lat'])

    input1 = mocker.MagicMock()

    output = cdat.process_input(operation, input1, process_func=process_func, FEAT_AXES=True)

    assert output == cdat.process_single_input.return_value

    cdat.process_single_input.assert_called_with(('time', 'lat'), process_func, input1)


def test_process_input_axes_missing_feat(mocker):
    process_func = mocker.MagicMock()

    operation = cwt.Process(identifier='CDAT.subset')

    operation.add_parameters(axes=['time', 'lat'])

    input1 = mocker.MagicMock()

    with pytest.raises(WPSError):
        cdat.process_input(operation, input1,  process_func=process_func)


def test_process_input_constant(mocker):
    process_func = mocker.MagicMock()

    operation = cwt.Process(identifier='CDAT.subset')

    operation.add_parameters(constant=10)

    input1 = mocker.MagicMock()

    orig_input1_variable = input1.variable

    output = cdat.process_input(operation, input1, process_func=process_func, FEAT_CONST=True)

    assert output == input1

    assert output.variable == process_func.return_value

    process_func.assert_called_with(orig_input1_variable, 10)


def test_process_input_constant_invalid(mocker):
    process_func = mocker.MagicMock()

    operation = cwt.Process(identifier='CDAT.subset')

    operation.add_parameters(constant='hello')

    input1 = mocker.MagicMock()

    with pytest.raises(WPSError):
        cdat.process_input(operation, input1,  process_func=process_func, FEAT_CONST=True)


def test_process_input_constant_missing_feat(mocker):
    process_func = mocker.MagicMock()

    operation = cwt.Process(identifier='CDAT.subset')

    operation.add_parameters(constant=10)

    input1 = mocker.MagicMock()

    with pytest.raises(WPSError):
        cdat.process_input(operation, input1,  process_func=process_func)


def test_process_input_multiple_inputs(mocker):
    mocker.patch.object(cdat, 'process_multiple_input')

    process_func = mocker.MagicMock()

    operation = cwt.Process(identifier='CDAT.subset')

    input1 = mocker.MagicMock()

    input2 = mocker.MagicMock()

    output = cdat.process_input(operation, input1, input2, process_func=process_func, FEAT_MULTI=True)

    assert output == cdat.process_multiple_input.return_value

    process_func.assert_not_called()

    cdat.process_multiple_input.assert_called_with(process_func, input1, input2)


def test_process_input_multiple_inputs_missing_feat(mocker):
    process_func = mocker.MagicMock()

    operation = cwt.Process(identifier='CDAT.subset')

    input1 = mocker.MagicMock()

    input2 = mocker.MagicMock()

    with pytest.raises(WPSError):
        cdat.process_input(operation, input1, input2, process_func=process_func)


def test_process_input(mocker):
    process_func = mocker.MagicMock()

    operation = cwt.Process(identifier='CDAT.subset')

    input1 = mocker.MagicMock()

    orig_input1_variable = input1.variable

    output = cdat.process_input(operation, input1, process_func=process_func)

    assert output == input1

    process_func.assert_called_with(orig_input1_variable)

    assert output.variable == process_func.return_value


def test_process_single_input(mocker):
    process_func = mocker.MagicMock()

    input = mocker.MagicMock()
    orig_input_variable = input.variable
    input.variable_axes = ['time', 'lat', 'lon']

    output = cdat.process_single_input(['time'], process_func, input)

    assert output == input

    assert input.variable == process_func.return_value

    process_func.assert_called_with(orig_input_variable, axis=(0,))

    input.remove_axis.assert_called_with('time')


def test_process_multiple_input_stack(mocker):
    mocker.patch.object(cdat, 'da')

    process_func = mocker.MagicMock()
    process_func.__name__ = 'sum_func'

    input1 = mocker.MagicMock()
    input2 = mocker.MagicMock()

    output = cdat.process_multiple_input(process_func, input1, input2)

    input1.copy.assert_called()

    cdat.da.stack.assert_called_with([input1.variable, input2.variable])

    process_func.assert_called_with(cdat.da.stack.return_value, axis=0)

    assert input1.copy.return_value.variable == process_func.return_value

    assert output == input1.copy.return_value


def test_process_multiple_input(mocker):
    process_func = mocker.MagicMock()
    process_func.__name__ = 'subset_func'

    input1 = mocker.MagicMock()
    input2 = mocker.MagicMock()

    output = cdat.process_multiple_input(process_func, input1, input2)

    input1.copy.assert_called()

    process_func.assert_called_with(input1.variable, input2.variable)

    assert input1.copy.return_value.variable == process_func.return_value

    assert output == input1.copy.return_value


def test_process_wrapper_func(mocker):
    mock_partial = mocker.MagicMock()
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
        'CDAT.aggregate': mock_partial,
        'CDAT.subset': mock_partial,
    }, True)
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'process')
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.object(cdat, 'DaskJobTracker')
    mocker.patch.object(managers, 'InputManager')
    mocker.patch.object(managers, 'FileManager')

    self = mocker.MagicMock()

    context = mocker.MagicMock()
    context.identifier = 'CDAT.subset'

    cdat.process_wrapper(self, context)

    cdat.process.assert_called_with(cdat.gather_inputs.return_value, context, process_func=mock_partial)


def test_process_wrapper_dask_cluster_error(mocker):
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'process')
    mocker.patch.object(managers, 'FileManager')

    cdat.Client.side_effect = OSError()

    self = mocker.MagicMock()

    context = mocker.MagicMock()
    context.identifier = 'CDAT.aggregate'

    with pytest.raises(DaskClusterAccessError):
        cdat.process_wrapper(self, context)


def test_process_wrapper_execute_error(mocker):
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'process')
    mocker.patch.object(managers, 'InputManager')
    mocker.patch.object(managers, 'FileManager')
    mocker.patch.object(cdat, 'DaskJobTracker')

    self = mocker.MagicMock()

    input1 = mocker.MagicMock()
    input2 = mocker.MagicMock()

    context = mocker.MagicMock()
    context.identifier = 'CDAT.aggregate'
    context.inputs = [input1, input2]
    context.extra = {
        'DASK_SCHEDULER': 'dask-scheduler',
    }

    cdat.Client.return_value.compute.side_effect = Exception()

    with pytest.raises(WPSError):
        cdat.process_wrapper(self, context)


def test_process_wrapper(mocker):
    mock_partial = mocker.MagicMock()
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
        'CDAT.subset': mock_partial,
    }, True)
    mocker.patch.object(cdat, 'Client')
    mocker.patch.object(cdat, 'process')
    mocker.patch.object(cdat, 'DaskJobTracker')
    mocker.patch.object(cdat, 'gather_inputs')
    mocker.patch.object(managers, 'InputManager')
    mocker.patch.object(managers, 'FileManager')

    self = mocker.MagicMock()

    input1 = mocker.MagicMock()
    input2 = mocker.MagicMock()

    context = mocker.MagicMock()
    context.identifier = 'CDAT.aggregate'
    context.inputs = [input1, input2]
    context.extra = {
        'DASK_SCHEDULER': 'dask-scheduler',
    }

    cdat.process_wrapper(self, context)

    cdat.Client.assert_called_with('dask-scheduler.default.svc:8786')

    cdat.process.assert_called_with(cdat.gather_inputs.return_value, context)

    to_xarray = cdat.process.return_value.to_xarray

    to_xarray.assert_called()

    to_netcdf = to_xarray.return_value.to_netcdf

    to_netcdf.assert_called_with(context.build_output_variable.return_value, compute=False)

    cdat.Client.return_value.compute.assert_called_with(to_netcdf.return_value)


def test_register_processes(mocker):
    mock_partial = mocker.MagicMock()
    mocker.spy(builtins, 'setattr')
    mocker.patch.object(base, 'cwt_shared_task')
    mocker.patch.object(base, 'register_process')
    mocker.patch.object(cdat, 'render_abstract')
    mocker.patch.object(cdat, 'Environment')
    mocker.patch.dict(cdat.PROCESS_FUNC_MAP, {
        'CDAT.aggregate': mock_partial,
        'CDAT.subset': mock_partial,
    }, True)

    cdat.register_processes()

    base.cwt_shared_task.assert_called()
    base.cwt_shared_task.return_value.assert_called_with(cdat.process_wrapper)

    template = cdat.Environment.return_value.from_string.return_value

    cdat.render_abstract.assert_called_with(cdat.DESCRIPTION_MAP['CDAT.subset'], mock_partial, template)

    base.register_process.assert_called_with('CDAT', 'subset', abstract=cdat.render_abstract.return_value, inputs=1)
    base.register_process.return_value.assert_called_with(base.cwt_shared_task.return_value.return_value)

    builtins.setattr.assert_any_call(cdat, 'subset_func', base.register_process.return_value.return_value)
