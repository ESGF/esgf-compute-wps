#! /usr/bin/env python

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
    raise WPSError('Workflows have been disabled')

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


def process_single(context, process):
    """ Process a single variable.

    Initialize the cluster workers, create the dask graph then execute.

    Args:
        context: A cwt.context.OperationContext.
        process: A function can be used to apply addition operations the the dask.Array.

    Returns:
        An updated cwt.context.OperationContext.
    """
    manager = init(context, settings.DASK_WORKERS)

    fm = managers.FileManager(context.user)

    input = managers.InputManager.from_cwt_variable(fm, context.inputs[0])

    input.map_domain(context.domain)

    input.subset()

    context.job.update('Subset data shape {!r}', input.data.shape)

    if process is not None:
        input.process(context.operation.get_parameter('axes', True).values, process)

        context.job.update('Process data shape {!r}', input.data.shape)

    if context.is_regrid:
        gridder = context.operation.get_parameter('gridder')

        input.regrid(gridder)

        context.job.update('Regrid data shape {!r}', input.data.shape)

    dataset = input.to_xarray()

    # Create the output directory
    context.output_path = context.gen_public_path()

    logger.info('Writing output to %r', context.output_path)

    try:
        # Execute the dask graph
        delayed = dataset.to_netcdf(context.output_path, compute=False)

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
    manager = init(context, settings.DASK_WORKERS)

    fm = managers.FileManager(context.user)

    input = managers.InputManager.from_cwt_variables(fm, context.inputs)

    input.map_domain(context.domain)

    input.subset()

    context.job.update('Subset data shape {!r}', input.data.shape)

    if context.is_regrid:
        gridder = context.operation.get_parameter('gridder')

        input.regrid(gridder)

        context.job.update('Regrid data shape {!r}', input.data.shape)

    dataset = input.to_xarray()

    # Create the output directory
    context.output_path = context.gen_public_path()

    logger.info('Writing output to %r', context.output_path)

    try:
        # Execute the dask graph
        delayed = dataset.to_netcdf(context.output_path, compute=False)

        fut = manager.client.compute(delayed)

        DaskJobTracker(context, fut)
    except Exception as e:
        raise WPSError('Error executing process: {!r}', e)

    return context


@base.register_process('CDAT', 'regrid', abstract=REGRID_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def regrid_func(self, context):
    """ Regrid Celery task.
    """
    # Ensure gridder is provided
    context.operation.get_parameter('gridder', True)

    return subset_func(context)


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
    return process_single(context, da.sum)


@base.register_process('CDAT', 'max', abstract=MAX_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def max_func(self, context):
    """ Max Celery task.
    """
    return process_single(context, da.max)


@base.register_process('CDAT', 'min', abstract=MIN_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def min_func(self, context):
    """ Min Celery task.
    """
    return process_single(context, da.min)
