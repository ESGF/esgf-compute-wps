#! /usr/bin/env python

from functools import partial

import cwt
import dask
import dask.array as da
from celery.utils.log import get_task_logger
from django.conf import settings
from dask.distributed import Client
from dask.distributed import LocalCluster
from distributed.diagnostics.progressbar import ProgressBar
from distributed.utils import LoopRunner
from distributed.client import futures_of
from tornado.ioloop import IOLoop

from cwt_kubernetes.cluster import Cluster
from cwt_kubernetes.cluster_manager import ClusterManager
from wps import WPSError
from wps.tasks import base
from wps.tasks import managers
from wps.tasks.dask_serialize import regrid_chunk

logger = get_task_logger('wps.tasks.cdat')


class DaskJobTracker(ProgressBar):
    def __init__(self, context, futures, scheduler=None, interval='100ms', complete=True):
        futures = futures_of(futures)

        if not isinstance(futures, (list, set)):
            futures = [futures, ]

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
            self.context.job.update('Processing', percent=percent)

            self.last = percent

    def _draw_stop(self, **kwargs):
        pass


def init(context, n_workers):
    if settings.DEBUG:
        cluster = LocalCluster(n_workers=2, threads_per_worker=2)

        client = Client(cluster) # noqa

        logger.info('Initialized cluster %r', cluster)

        manager = ClusterManager(client.scheduler.address, None)
    else:
        cluster = Cluster.from_yaml(settings.DASK_KUBE_NAMESPACE, '/etc/config/dask/worker-spec.yml')

        manager = ClusterManager(settings.DASK_SCHEDULER, cluster)

        labels = {
            'user': str(context.user.id),
        }

        context.job.update('Initialize {!r} dask workers', n_workers)

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
    fm = managers.FileManager(context.user)

    # Topologically sort the operations
    topo_order = context.topo_sort()

    # Hold the intermediate inputs
    interm = {}

    while topo_order:
        next = topo_order.pop(0)

        # if all inputs are variables then create a new InputManager
        if all(isinstance(x, cwt.Variable) for x in next.inputs):
            input = managers.InputManager.from_cwt_variables(fm, next.inputs)

            # Subset the inputs
            input.subset(next.domain)

            # If the current process is in the function map then apply the process
            if next.identifier in PROCESS_FUNC_MAP:
                PROCESS_FUNC_MAP[next.identifier](next, input)

            # Regrid to the target grid
            if next.gridder is not None:
                regrid(next, input)

            # Add to intermediate store
            interm[next.name] = input
        else:
            # Grab all the intermediate inputs
            try:
                data = [interm[x.name] for x in next.inputs]
            except KeyError as e:
                raise WPSError('Unable to find intermidiate {!s}', e)

            if len(data) > 1:
                raise WPSError('Multiple inputs not supported')
            else:
                data = data[0]

            # Make a new copy
            input = data.copy()

            # Apply processing if needed
            if next.identifier in PROCESS_FUNC_MAP:
                PROCESS_FUNC_MAP[next.identifier](next, input)

            # Regrid to target grid
            if next.gridder is not None:
                regrid(next, input)

            # Add new input to intermediate store
            interm[next.name] = input

    # Initialize the cluster resources
    manager = init(context, settings.DASK_WORKERS)

    try:
        delayed = []

        for output in context.output_ops():
            # Create an output xarray Dataset
            dataset = interm[output.name].to_xarray()

            output_name = '{!s}-{!s}'.format(output.name, output.identifier)

            local_path = context.build_output_variable(interm[output.name].var_name, name=output_name)

            # Create an output file and store the future
            delayed.append(dataset.to_netcdf(local_path, compute=False))

        # Execute the futures
        fut = manager.client.compute(delayed)

        # Track the progress
        DaskJobTracker(context, fut)
    except Exception as e:
        raise WPSError('Error executing process: {!r}', e)

    return context


REGRID_ABSTRACT = """
Regrids a variable to designated grid. Required parameter named "gridder".
"""

SUBSET_ABSTRACT = """
Subset a variable by provided domain. Supports regridding.
"""

AGGREGATE_ABSTRACT = """
Aggregate a variable over multiple files. Supports subsetting and regridding.
"""

AVERAGE_ABSTRACT = """
Computes the average over axes.

Required parameters:
 axes: A list of axes to operate on. Should be separated by "|".

Optional parameters:
 weightoptions: A string whos value is "generate",
   "equal", "weighted", "unweighted". See documentation
   at https://cdat.llnl.gov/documentation/utilities/utilities-1.html
"""

SUM_ABSTRACT = """
Computes the sum over an axis. Requires singular parameter named "chunked_axis, axes"
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
"""

MAX_ABSTRACT = """
Computes the maximum over an axis. Requires singular parameter named "chunked_axis, axes"
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
"""

MIN_ABSTRACT = """
Computes the minimum over an axis. Requires singular parameter named "chunked_axis, axes"
whose value will be used to process over. The value should be a "|" delimited
string e.g. 'lat|lon'.
"""


def process_single(context, process=None, aggregate=False):
    """ Process a single variable.

    Initialize the cluster workers, create the dask graph then execute.

    The `process` function should have the following signature
    (cwt.Operation, wps.tasks.managers.InputManager).

    Args:
        context: A cwt.context.OperationContext.
        process: A custom processing function to be applied.
        aggregate: A bool value to treat the inputs as an aggregation.

    Returns:
        An updated cwt.context.OperationContext.
    """
    manager = init(context, settings.DASK_WORKERS)

    fm = managers.FileManager(context.user)

    if aggregate:
        input = managers.InputManager.from_cwt_variables(fm, context.inputs)
    else:
        input = managers.InputManager.from_cwt_variable(fm, context.inputs[0])

    input.subset(context.domain)

    context.job.update('Subset data shape {!r}', input.data.shape)

    if process is not None:
        process(context.operation, input)

    if context.is_regrid:
        regrid(context.operation, input)

    dataset = input.to_xarray()

    local_path = context.build_output_variable(input.var_name)

    logger.info('Writing output to %r', local_path)

    try:
        # Execute the dask graph
        delayed = dataset.to_netcdf(local_path, compute=False)

        fut = manager.client.compute(delayed)

        DaskJobTracker(context, fut)
    except Exception as e:
        raise WPSError('Error executing process: {!r}', e)

    return context


@base.register_process('CDAT', 'subset', abstract=SUBSET_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def subset_func(self, context):
    """ Subset Celery task.
    """
    return process_single(context, None)


@base.register_process('CDAT', 'aggregate', abstract=AGGREGATE_ABSTRACT, metadata={'inputs': '*'})
@base.cwt_shared_task()
def aggregate_func(self, context):
    """ Aggregate Celery task.
    """
    return process_single(context, None, True)


@base.register_process('CDAT', 'regrid', abstract=REGRID_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def regrid_func(self, context):
    """ Regrid Celery task.
    """
    # Ensure gridder is provided
    context.operation.get_parameter('gridder', True)

    return process_single(context, None)


# @base.register_process('CDAT', 'average', abstract=AVERAGE_ABSTRACT, metadata={'inputs': '1'})
# @base.cwt_shared_task()
def average_func(self, context):
    init(context, settings.DASK_WORKERS)

    return context


@base.register_process('CDAT', 'sum', abstract=SUM_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def sum_func(self, context):
    """ Sum Celery task.
    """
    return process_single(context, PROCESS_FUNC_MAP['CDAT.sum'])


@base.register_process('CDAT', 'max', abstract=MAX_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def max_func(self, context):
    """ Max Celery task.
    """
    return process_single(context, PROCESS_FUNC_MAP['CDAT.max'])


@base.register_process('CDAT', 'min', abstract=MIN_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def min_func(self, context):
    """ Min Celery task.
    """
    return process_single(context, PROCESS_FUNC_MAP['CDAT.min'])


def regrid(operation, input):
    gridder = operation.get_parameter('gridder', True)

    if len(input.vars) == 0:
        input.load_variables_and_axes()

    selector = dict((x, (input.axes[x][0], input.axes[x][-1])) for x in input.map.keys())

    grid, tool, method = input.regrid_context(gridder, selector)

    delayed = input.data.to_delayed().squeeze()

    if delayed.size == 1:
        delayed = delayed.reshape((1, ))

    # Create list of axis data that is present in the selector.
    axis_data = [input.axes[x] for x in selector.keys()]

    regrid_delayed = [dask.delayed(regrid_chunk)(x, axis_data, grid, tool, method) for x in delayed]

    regrid_arrays = [da.from_delayed(x, y.shape[:-2] + grid.shape, input.data.dtype)
                     for x, y in zip(regrid_delayed, input.data.blocks)]

    input.data = input.vars[input.var_name] = da.concatenate(regrid_arrays)

    logger.info('Concatenated regridded arrays %r', input.data)

    input.axes['lat'] = grid.getLatitude()

    input.vars['lat_bnds'] = input.axes['lat'].getBounds()

    input.axes['lon'] = grid.getLongitude()

    input.vars['lon_bnds'] = input.axes['lon'].getBounds()


def process(operation, input, process_func):
    axes = operation.get_parameter('axes', True).values

    map_keys = list(input.map.keys())

    indices = tuple(map_keys.index(x) for x in axes)

    logger.info('Mapped axes %r to indices %r', axes, indices)

    input.data = process_func(input.data, axis=indices)

    logger.info('Processed data %r', input.data)

    for axis in axes:
        logger.info('Removing axis %r', axis)

        input.map.pop(axis)


PROCESS_FUNC_MAP = {
    'CDAT.sum': partial(process, process_func=da.sum),
    'CDAT.max': partial(process, process_func=da.max),
    'CDAT.min': partial(process, process_func=da.min),
    'CDAT.regrid': regrid,
}
