#! /usr/bin/env python

import os
from functools import partial

import cwt
import dask
import dask.array as da
from celery.utils.log import get_task_logger
from dask.distributed import Client
from dask.distributed import LocalCluster
from distributed.diagnostics.progressbar import ProgressBar
from distributed.utils import LoopRunner
from distributed.client import futures_of
from tornado.ioloop import IOLoop

from cwt_kubernetes.cluster import Cluster
from cwt_kubernetes.cluster_manager import ClusterManager
from compute_tasks import base
from compute_tasks import context as ctx
from compute_tasks import managers
from compute_tasks import WPSError
from compute_tasks.dask_serialize import regrid_chunk

logger = get_task_logger('compute_tasks.cdat')

DEV = os.environ.get('DEV', False)
DASK_KUBE_NAMESPACE = os.environ.get('DASK_KUBE_NAMESPACE', 'default')
DASK_SCHEDULER = os.environ.get('DASK_SCHEDULER', '')
DASK_WORKERS = os.environ.get('DASK_WORKERS', 10)


class DaskJobTracker(ProgressBar):
    def __init__(self, context, futures, scheduler=None, interval='100ms', complete=True):
        futures = futures_of(futures)

        if not isinstance(futures, (list, set)):
            futures = [futures, ]

        context.message('Tracking {!r} futures', len(futures))

        super(DaskJobTracker, self).__init__(futures, scheduler, interval, complete)

        self.context = context
        self.last = None

        self.loop = IOLoop()

        loop_runner = LoopRunner(self.loop)
        loop_runner.run_sync(self.listen)

    def _draw_bar(self, remaining, all, **kwargs):
        frac = (1 - remaining / all) if all else 1.0

        percent = int(100 * frac)

        logger.debug('Percent processed %r', percent)

        if self.last is None or self.last != percent:
            self.context.message('Processing', percent=percent)

            self.last = percent

    def _draw_stop(self, **kwargs):
        pass


def init(context, n_workers):
    if DEV:
        cluster = LocalCluster(n_workers=2, threads_per_worker=2, processes=False)

        client = Client(cluster) # noqa

        logger.info('Initialized cluster %r', cluster)

        manager = ClusterManager(client.scheduler.address, None)
    else:
        cluster = Cluster.from_yaml(DASK_KUBE_NAMESPACE, '/etc/config/dask/worker-spec.yml')

        manager = ClusterManager(DASK_SCHEDULER, cluster)

        labels = {
            'user': str(context.user),
        }

        context.message('Initializing {!r} workers', n_workers)

        manager.scale_up_workers(n_workers, labels)

    return manager


@base.register_process('CDAT', 'workflow', metadata={'inputs': '0'})
@base.cwt_shared_task()
def workflow_func(self, context):
    """ Executes a workflow.

    Process a forest of operations. The graph can have multiple inputs and
    outputs.

    Args:
        context (WorkflowOperationContext): Current context.

    Returns:
        Updated context.
    """
    fm = managers.FileManager(context)

    # Topologically sort the operations
    topo_order = context.topo_sort()

    # Hold the intermediate inputs
    interm = {}

    while topo_order:
        next = topo_order.pop(0)

        context.message('Processing operation {!r} - {!r}', next.name, next.identifier)

        # if all inputs are variables then create a new InputManager
        if all(isinstance(x, cwt.Variable) for x in next.inputs):
            input = managers.InputManager.from_cwt_variables(fm, next.inputs)

            # Subset the inputs
            src, subset = input.subset(next.domain)

            context.track_src_bytes(src.nbytes)

            context.track_in_bytes(subset.nbytes)

            # If the current process is in the function map then apply the process
            if next.identifier in PROCESS_FUNC_MAP:
                output = PROCESS_FUNC_MAP[next.identifier](next, *[input, ])
            else:
                output = input

            # Regrid to the target grid
            if next.gridder is not None:
                regrid(next, output)

            # Add to intermediate store
            interm[next.name] = output
        else:
            # Grab all the intermediate inputs
            try:
                inputs = [interm[x.name] for x in next.inputs]
            except KeyError as e:
                raise WPSError('Unable to find intermidiate {!s}', e)

            # Create copy of single input, multiple inputs automatically create a new output
            if len(inputs) == 1:
                inputs = [inputs[0].copy(), ]

            try:
                output = PROCESS_FUNC_MAP[next.identifier](next, *inputs)
            except KeyError as e:
                raise WPSError('Operation {!s} is not supported in a workflow', e)
            except Exception:
                raise

            # Regrid to target grid
            if next.gridder is not None:
                regrid(next, output)

            # Add new input to intermediate store
            interm[next.name] = output

        context.message('Storing intermediate {!r}', next.name)

    context.message('Preparing to execute workflow')

    # Initialize the cluster resources
    manager = init(context, DASK_WORKERS)

    try:
        delayed = []

        for output in context.output_ops():
            context.track_out_bytes(interm[output.name].nbytes)

            # Create an output xarray Dataset
            dataset = interm[output.name].to_xarray()

            output_name = '{!s}-{!s}'.format(output.name, output.identifier)

            local_path = context.build_output_variable(interm[output.name].var_name, name=output_name)

            context.message('Building output for {!r} - {!r}', output.name, output.identifier)

            logger.debug('Writing local output to %r', local_path)

            # Create an output file and store the future
            delayed.append(dataset.to_netcdf(local_path, compute=False))

        context.message('Executing workflow')

        with ctx.ProcessTimer(context):
            # Execute the futures
            fut = manager.client.compute(delayed)

            # Track the progress
            DaskJobTracker(context, fut)
    except Exception as e:
        raise WPSError('Error executing process: {!r}', e)

    return context


def process(context, process=None, aggregate=False):
    """ Process a single variable.

    Initialize the cluster workers, create the dask graph then execute.

    The `process` function should have the following signature
    (cwt.Operation, compute_tasks.managers.InputManager).

    Args:
        context: A cwt.context.OperationContext.
        process: A custom processing function to be applied.
        aggregate: A bool value to treat the inputs as an aggregation.

    Returns:
        An updated cwt.context.OperationContext.
    """
    manager = init(context, DASK_WORKERS)

    fm = managers.FileManager(context)

    if aggregate:
        inputs = [managers.InputManager.from_cwt_variables(fm, context.inputs), ]

        context.message('Building aggregation with {!r} inputs', len(context.inputs))
    else:
        inputs = [managers.InputManager.from_cwt_variable(fm, x) for x in context.inputs]

        context.message('Building {!r} inputs', len(inputs))

    for input in inputs:
        src, subset = input.subset(context.domain)

        context.track_src_bytes(src.nbytes)

        context.track_in_bytes(subset.nbytes)

        context.message('Data subset shape {!r}', input.data.shape)

    if process is not None:
        output = process(context.operation, *inputs)

        context.message('Applied process {!r} shape {!r}', context.operation.identifier, output.data.shape)
    else:
        output = inputs[0]

    if context.is_regrid:
        regrid(context.operation, output)

        context.message('Applied regridding shape {!r}', output.data.shape)

    context.message('Preparing to execute')

    context.track_out_bytes(output.data.nbytes)

    dataset = output.to_xarray()

    local_path = context.build_output_variable(output.var_name)

    logger.debug('Writing output to %r', local_path)

    try:
        # Execute the dask graph
        delayed = dataset.to_netcdf(local_path, compute=False)

        context.message('Executing')

        with ctx.ProcessTimer(context):
            fut = manager.client.compute(delayed)

            DaskJobTracker(context, fut)
    except Exception as e:
        raise WPSError('Error executing process: {!r}', e)

    return context


SUBSET_ABSTRACT = """
Subset a variable by provided domain. Supports regridding.
"""


@base.register_process('CDAT', 'subset', abstract=SUBSET_ABSTRACT, inputs=1)
@base.cwt_shared_task()
def subset_func(self, context):
    """ Subset Celery task.
    """
    return process(context)


AGGREGATE_ABSTRACT = """
Aggregate a variable over multiple files. Supports subsetting and regridding.
"""


@base.register_process('CDAT', 'aggregate', abstract=AGGREGATE_ABSTRACT, inputs=50)
@base.cwt_shared_task()
def aggregate_func(self, context):
    """ Aggregate Celery task.
    """
    return process(context, aggregate=True)


REGRID_ABSTRACT = """
Regrids a variable to designated grid. Required parameter named "gridder".
"""


@base.register_process('CDAT', 'regrid', abstract=REGRID_ABSTRACT, inputs=1)
@base.cwt_shared_task()
def regrid_func(self, context):
    """ Regrid Celery task.
    """
    # Ensure gridder is provided
    context.operation.get_parameter('gridder', True)

    return process(context)


AXES_PARAM = 'axes: A list of axes to operate on. Multiple values should be separated by "|" e.g. "lat|lon".'


CONST_PARAM = 'constant: An integer or float value that will be applied element-wise.'


AVERAGE_ABSTRACT = """
Computes the average over axes.

Required parameters:
 axes: A list of axes to operate on. Should be separated by "|".

Optional parameters:
 weightoptions: A string whos value is "generate",
   "equal", "weighted", "unweighted". See documentation
   at https://cdat.llnl.gov/documentation/utilities/utilities-1.html
"""


# @base.register_process('CDAT', 'average', abstract=AVERAGE_ABSTRACT, metadata={'inputs': '1'})
# @base.cwt_shared_task()
def average_func(self, context):
    return context


SUM_ABSTRACT = """
Computes the sum over an axis, two inputs or a constant.

Optional parameters:
 {!s}
 {!s}
""".format(AXES_PARAM, CONST_PARAM)


@base.register_process('CDAT', 'sum', abstract=SUM_ABSTRACT, inputs=2)
@base.cwt_shared_task()
def sum_func(self, context):
    """ Sum Celery task.
    """
    return process(context, PROCESS_FUNC_MAP['CDAT.sum'])


MAX_ABSTRACT = """
Computes the max over an axis, two inputs or a constant.

Optional parameters:
 {!s}
 {!s}
""".format(AXES_PARAM, CONST_PARAM)


@base.register_process('CDAT', 'max', abstract=MAX_ABSTRACT, inputs=2)
@base.cwt_shared_task()
def max_func(self, context):
    """ Max Celery task.
    """
    return process(context, PROCESS_FUNC_MAP['CDAT.max'])


MIN_ABSTRACT = """
Computes the min over an axis, two inputs or a constant.

Optional parameters:
 {!s}
 {!s}
""".format(AXES_PARAM, CONST_PARAM)


@base.register_process('CDAT', 'min', abstract=MIN_ABSTRACT, inputs=2)
@base.cwt_shared_task()
def min_func(self, context):
    """ Min Celery task.
    """
    return process(context, PROCESS_FUNC_MAP['CDAT.min'])


SUBTRACT_ABSTRACT = """
Computes subtract between two inputs or a constant.

Optional parameters:
 {!s}
""".format(CONST_PARAM)


@base.register_process('CDAT', 'subtract', abstract=SUBTRACT_ABSTRACT, inputs=2)
@base.cwt_shared_task()
def subtract_func(self, context):
    """ Subtract Celery task.
    """
    return process(context, PROCESS_FUNC_MAP['CDAT.subtract'])


ADD_ABSTRACT = """
Computes add between two inputs or a constant.

Optional parameters:
 {!s}
""".format(CONST_PARAM)


@base.register_process('CDAT', 'add', abstract=ADD_ABSTRACT, inputs=2)
@base.cwt_shared_task()
def add_func(self, context):
    """ Subtract Celery task.
    """
    return process(context, PROCESS_FUNC_MAP['CDAT.add'])


def regrid(operation, *inputs):
    gridder = operation.get_parameter('gridder', True)

    input = inputs[0]

    if len(input.vars) == 0:
        input.load_variables_and_axes()

    selector = dict((x, (input.axes[x][0], input.axes[x][-1])) for x in input.map.keys())

    grid, tool, method = input.regrid_context(gridder, selector)

    delayed = input.data.to_delayed().squeeze()

    if delayed.size == 1:
        delayed = delayed.reshape((1, ))

    logger.debug('Converted dask graph to %r delayed', delayed.shape)

    # Create list of axis data that is present in the selector.
    axis_data = [input.axes[x] for x in selector.keys()]

    logger.info('Creating regrid_chunk delayed functions')

    regrid_delayed = [dask.delayed(regrid_chunk)(x, axis_data, grid, tool, method) for x in delayed]

    logger.info('Converting delayed functions to dask arrays')

    regrid_arrays = [da.from_delayed(x, y.shape[:-2] + grid.shape, input.data.dtype)
                     for x, y in zip(regrid_delayed, input.data.blocks)]

    input.data = input.vars[input.var_name] = da.concatenate(regrid_arrays)

    logger.info('Concatenated %r arrays to output %r', len(regrid_arrays), input.data)

    input.axes['lat'] = grid.getLatitude()

    input.vars['lat_bnds'] = input.axes['lat'].getBounds()

    input.axes['lon'] = grid.getLongitude()

    input.vars['lon_bnds'] = input.axes['lon'].getBounds()


def process_input(operation, *inputs, process_func=None, **supported): # noqa E999
    axes = operation.get_parameter('axes')

    constant = operation.get_parameter('constant')

    logger.info('Axes %r constant %r', axes, constant)

    if axes is not None:
        if not supported.get('single_input_axes', True):
            raise WPSError('Axes parameter is not supported by operation {!r}', operation.identifier)

        output = process_single_input(axes.values, process_func, inputs[0])
    elif constant is not None:
        if not supported.get('single_input_constant', True):
            raise WPSError('Constant parameter is not supported by operation {!r}', operation.identifier)

        try:
            constant = float(constant.values[0])
        except ValueError:
            raise WPSError('Invalid constant value {!r} type {!s} expecting <class \'float\'>',
                           constant, type(constant))

        output = inputs[0]

        output.data = process_func(output.data, constant)

        logger.info('Process output %r', output.data)
    elif len(inputs) > 1:
        if not supported.get('multiple_input', True):
            raise WPSError('Multiple inputs are not supported by operation {!r}', operation.identifier)

        output = process_multiple_input(process_func, *inputs)
    else:
        logger.debug('Axes %r Constant %r Supported %r', axes, constant, supported)

        logger.debug('Process func %r', process_func)

        logger.debug('Inputs %r', inputs)

        raise WPSError('Error configuring operation {!r} might be missing axes or constant parameters',
                       operation.identifier)

    return output


def process_single_input(axes, process_func, input):
    map_keys = list(input.map.keys())

    indices = tuple(map_keys.index(x) for x in axes)

    logger.info('Mapped axes %r to indices %r', axes, indices)

    input.data = process_func(input.data, axis=indices)

    logger.info('Process output %r', input.data)

    for axis in axes:
        logger.info('Removing axis %r', axis)

        input.remove_axis(axis)

    return input


def process_multiple_input(process_func, input1, input2):
    new_input = input1.copy()

    if process_func.__name__ in REQUIRES_STACK:
        stacked = da.stack([input1.data, input2.data])

        logger.info('Stacking inputs %r', stacked)

        new_input.data = process_func(stacked, axis=0)

    else:
        new_input.data = process_func(input1.data, input2.data)

    logger.info('Process output %r', new_input.data)

    return new_input


REQUIRES_STACK = [
    da.sum.__name__,
    da.max.__name__,
    da.min.__name__,
]


PROCESS_FUNC_MAP = {
    'CDAT.sum': partial(process_input, process_func=da.sum),
    'CDAT.max': partial(process_input, process_func=da.max),
    'CDAT.min': partial(process_input, process_func=da.min),
    'CDAT.regrid': regrid,
    'CDAT.subtract': partial(process_input, process_func=da.subtract, single_input_axes=False),
    'CDAT.add': partial(process_input, process_func=da.add, singe_input_axes=False),
}
