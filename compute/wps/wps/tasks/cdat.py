#! /usr/bin/env python

from builtins import str
import datetime
import os
import math
from collections import OrderedDict

import cdms2
import cwt
import cdutil
import dask
import dask.array as da
import xarray as xr
from cdms2 import MV2
from celery.utils.log import get_task_logger
from django.conf import settings
from dask.distributed import Client

from wps import metrics
from wps import WPSError
from wps.tasks import base
from wps.tasks import credentials
from wps.context import OperationContext
from cwt_kubernetes.cluster import Cluster
from cwt_kubernetes.cluster_manager import ClusterManager

logger = get_task_logger('wps.tasks.cdat')

BACKEND = 'CDAT'


@base.register_process('CDAT', 'workflow', metadata={'inputs': '0'})
@base.cwt_shared_task()
def workflow(self, context):
    """ Executes a workflow.

    Process a forest of operations. The graph can have multiple inputs and
    outputs.

    Args:
        context (WorkflowOperationContext): Current context.

    Returns:
        Updated context.
    """
    client = cwt.WPSClient(settings.WPS_ENDPOINT, api_key=context.user.auth.api_key,
                           verify=False)

    queue = context.build_execute_graph()

    while len(queue) > 0:
        next = queue.popleft()

        completed = context.wait_for_inputs(next)

        if len(completed) > 0:
            completed_ids = ', '.join('-'.join([x.identifier, x.name]) for x in completed)

            self.status('Processes {!s} have completed', completed_ids)

        context.prepare(next)

        # TODO distributed workflows
        # Here we can make the choice on which client to execute
        client.execute(next)

        context.add_executing(next)

        self.status('Executing process {!s}-{!s}', next.identifier, next.name)

    completed = context.wait_remaining()

    if len(completed) > 0:
        completed_ids = ', '.join('-'.join([x.identifier, x.name]) for x in completed)

        self.status('Processes {!s} have completed', completed_ids)

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


def process_data(self, context, index, process):
    """ Process a chunks of data.

    Function passed as process should accept two arguments, the first being a
    cdms2.TransientVariable and the second a list of axis names.

    Args:
        context (OperationContext): Current context.
        index (int): Worker index used to determine the portion of work to complete.
        process (function): A function to process the data, see above for arguments.

    Returns:
        Updated context.
    """
    axes = context.operation.get_parameter('axes', True)

    nbytes = 0

    for input_index, input in enumerate(context.sorted_inputs()):
        for _, chunk_index, chunk in input.chunks(index, context):
            nbytes += chunk.nbytes

            process_filename = '{}_{:08}_{:08}_{}.nc'.format(
                str(context.job.id), input_index, chunk_index, '_'.join(axes.values))

            cache_path = context.gen_cache_path(process_filename)

            if process is not None:
                # Track processing time
                with metrics.WPS_PROCESS_TIME.labels(context.operation.identifier).time():
                    chunk = process(chunk, axes.values)

            with context.new_output(cache_path) as outfile:
                outfile.write(chunk, id=input.variable.var_name)

            input.process.append(cache_path)

    self.status('Processed {!r} bytes', nbytes)

    return context


def parse_filename(path):
    """ Parses filename from path.

    /data/hello.nc -> hello

    Args:
        path (string): A path to parse.

    Retunrs:
        str: The base filename
    """
    base = os.path.basename(path)

    filename, _ = os.path.splitext(base)

    return filename


def regrid_chunk(context, chunk, selector):
    """ Regrids a chunk of data.

    Args:
        context (OperationContext): Current context.
        chunk (cdms2.TransientVariable): Chunk of data to be regridded.
        selector (dict): A dict describing the portion of data we want.

    Returns:
        cdms2.TransientVariable: Regridded varibale.
    """
    grid, tool, method = context.regrid_context(selector)

    shape = chunk.shape

    chunk = chunk.regrid(grid, regridTool=tool, regridMethod=method)

    logger.info('Regrid %r -> %r', shape, chunk.shape)

    return chunk


@base.cwt_shared_task()
def concat(self, contexts):
    """ Concatenate data chunks.

    Args:
        context (OperationContext): Current context.

    Returns:
        Updated context.
    """
    context = OperationContext.merge_ingress(contexts)

    context.output_path = context.gen_public_path()

    nbytes = 0
    start = datetime.datetime.now()

    with context.new_output(context.output_path) as outfile:
        for index, input in enumerate(context.sorted_inputs()):
            data = []
            chunk_axis = None
            chunk_axis_index = None

            # Skip file if not mapped
            if input.mapped is None:
                logger.info('Skipping %r', input.filename)

                continue

            for file_path, _, chunk in input.chunks(input_index=index, context=context):
                logger.info('Chunk shape %r %r', file_path, chunk.shape)

                if chunk_axis is None:
                    chunk_axis_index = chunk.getAxisIndex(input.chunk_axis)

                    chunk_axis = chunk.getAxis(chunk_axis_index)

                # We can write chunks along the temporal axis immediately
                # otherwise we need to collect them to concatenate over
                # an axis
                if chunk_axis.isTime():
                    logger.info('Writing temporal chunk %r', chunk.shape)

                    if context.units is not None:
                        chunk.getTime().toRelativeTime(str(context.units))

                    if context.is_regrid:
                        chunk = regrid_chunk(context, chunk, input.mapped)

                    outfile.write(chunk, id=str(input.variable.var_name))
                else:
                    logger.info('Gathering spatial chunk')

                    data.append(chunk)

                nbytes += chunk.nbytes

            # Concatenate chunks along an axis
            if chunk_axis is not None and not chunk_axis.isTime():
                data = MV2.concatenate(data, axis=chunk_axis_index)

                if context.is_regrid:
                    chunk = regrid_chunk(context, chunk, input.mapped)

                outfile.write(data, id=str(input.variable.var_name))

                nbytes += chunk.nbytes

    elapsed = datetime.datetime.now() - start

    self.status('Processed {!r} bytes in {!r} seconds', nbytes, elapsed.total_seconds())

    return context


@base.register_process('CDAT', 'regrid', abstract=REGRID_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def regrid(self, context, index):
    """ Regrids a chunk of data.
    """
    return context

client = Client(os.environ['DASK_SCHEDULER']) # noqa

cluster = Cluster.from_yaml('default', 'worker-spec.yml')


def format_dimension(dim):
    if dim.crs == cwt.VALUES:
        data = (dim.start, dim.end, dim.step)
    elif dim.crs == cwt.INDICES:
        data = slice(dim.start, dim.end, dim.step)
    elif dim.crs == cwt.TIMESTAMPS:
        data = (dim.start, dim.end, dim.step)
    else:
        raise WPSError('Unknown dimension CRS %r', dim)

    return data


def domain_to_dict(domain):
    if domain is None:
        return {}

    return dict((x, format_dimension(y)) for x, y in domain.dimensions.items())


def map_domain(url, var_name, domain, cert=None):
    if cert is not None:
        with open('cert.pem', 'w') as outfile:
            outfile.write(cert)

        import os

        cert_path = os.path.join(os.getcwd(), 'cert.pem')

        with open('.dodsrc', 'w') as outfile:
            outfile.write('HTTP.COOKIEJAR=.dods_cookies\n')
            outfile.write('HTTP.SSL.CERTIFICATE={}\n'.format(cert_path))
            outfile.write('HTTP.SSL.KEY={}\n'.format(cert_path))
            outfile.write('HTTP.SSL.VERIFY=0\n')

    import cdms2 # noqa

    axis_data = OrderedDict()

    with cdms2.open(url) as infile:
        for axis in infile[var_name].getAxisList():
            if axis.id in domain:
                if isinstance(domain[axis.id], slice):
                    axis_data[axis.id] = domain[axis.id]
                else:
                    axis_clone = axis.clone()
                    try:
                        interval = axis_clone.mapInterval(domain[axis.id][:2])
                    except TypeError:
                        raise WPSError('Failed to map axis %r', axis.id)
                    axis_data[axis.id] = slice(interval[0], interval[1], domain[axis.id][2])
            else:
                axis_data[axis.id] = slice(None, None, None)

    return url, axis_data


def calculate_chunking(domain, shape, axis):
    # TODO Replace 2 with the number of workers allocated for the user
    # Take ceiling so we can fit into x workers
    if domain[axis].stop is None and domain[axis].start is None:
        size = shape[axis] / 2
    else:
        size = math.ceil((domain[axis].stop - domain[axis].start) / 2)

    logger.info('Chunk size %r', size)

    index = list(domain.keys()).index(axis)

    old_shape = shape

    shape.pop(index)

    shape.insert(index, size)

    logger.info('Chunk shape %r -> %r', old_shape, shape)

    return shape


def output_with_attributes(f, subset_data, domain, var_name):
    var = {}
    coords = {}

    for v in f.getVariables():
        selector = {}

        a_coords = {}

        for a in v.getAxisList():
            if a.id not in coords:
                if a.id in domain:
                    data = a[domain[a.id]]
                else:
                    data = a[:]

                coords[a.id] = xr.DataArray(data, name=a.id, dims=a.id, attrs=a.attributes)

            if a.id in domain:
                selector[a.id] = domain[a.id]

            a_coords[a.id] = coords[a.id]

        if v.id == var_name:
            var_da = subset_data
        else:
            var_da = v(**selector)

        var[v.id] = xr.DataArray(var_da, name=v.id, dims=a_coords.keys(), coords=a_coords, attrs=a.attributes)

        if v.id == var_name:
            var[v.id].attrs['_FillValue'] = 1e20

    return xr.Dataset(var, attrs=f.attributes)


def retrieve_chunk(url, var_name, selector, cert):
    with open('cert.pem', 'w') as outfile:
        outfile.write(cert)

    import os

    cert_path = os.path.join(os.getcwd(), 'cert.pem')

    with open('.dodsrc', 'w') as outfile:
        outfile.write('HTTP.COOKIEJAR=.dods_cookies\n')
        outfile.write('HTTP.SSL.CERTIFICATE={}\n'.format(cert_path))
        outfile.write('HTTP.SSL.KEY={}\n'.format(cert_path))
        outfile.write('HTTP.SSL.VERIFY=0\n')

    import cdms2

    with cdms2.open(url) as infile:
        return infile(var_name, **selector)


def update_shape(shape, chunk, index):
    diff = chunk.stop - chunk.start

    shape.pop(index)

    shape.insert(index, diff)

    return tuple(shape)


def construct_ingress(var, cert, chunk_shape):
    url = var.parent.id

    size = var.getTime().shape[0]

    index = var.getAxisIndex('time')

    step = chunk_shape[index]

    chunks = [{'time': slice(x, min(x+step, size), 1)} for x in range(0, size, step)]

    da_chunks = [da.from_delayed(dask.delayed(retrieve_chunk)(url, var.id, x, cert),
                                 update_shape(list(var.shape), x['time'], 0),
                                 var.dtype)
                 for x in chunks]

    concat = da.concatenate(da_chunks, axis=0)

    return concat


@base.register_process('CDAT', 'subset', abstract=SUBSET_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def subset(self, context):
    """ Subsetting data.
    """
    input = context.inputs[0].variable

    cert = None

    if not context.inputs[0].check_access():
        cert_path = credentials.load_certificate(context.user)

        if context.inputs[0].check_access(cert_path):
            cert = context.user.auth.cert

    domain = domain_to_dict(context.domain)

    logger.info('Translated domain to %r', domain)

    mapped_delayed = dask.delayed(map_domain)(input.uri, input.var_name, domain, cert)

    url, map = mapped_delayed.compute()

    logger.info('Mapped domain to %r', map)

    infile = cdms2.open(input.uri)

    var = infile[input.var_name]

    chunks = calculate_chunking(map, list(var.shape), 'time')

    logger.info('Setting chunk to %r', chunks)

    if cert is None:
        data = da.from_array(var, chunks=chunks)
    else:
        data = construct_ingress(var, cert, chunks)

    selector = tuple(map.values())

    logger.info('Subsetting variable %r with selector %r', input.var_name, selector)

    subset_data = data[selector]

    dataset = output_with_attributes(infile, subset_data, map, input.var_name)

    output_path = context.gen_public_path()

    dataset.to_netcdf(output_path)

    infile.close()

    return context


@base.register_process('CDAT', 'aggregate', abstract=AGGREGATE_ABSTRACT, metadata={'inputs': '*'})
@base.cwt_shared_task()
def aggregate(self, context, index):
    """ Aggregating data.
    """
    return context


@base.register_process('CDAT', 'average', abstract=AVERAGE_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def average(self, context, index):
    def average_func(data, axes):
        axis_indices = []

        for axis in axes:
            axis_index = data.getAxisIndex(axis)

            if axis_index == -1:
                raise WPSError('Unknown axis {!s}', axis)

            axis_indices.append(str(axis_index))

        axis_sig = ''.join(axis_indices)

        data = cdutil.averager(data, axis=axis_sig)

        return data

    return process_data(self, context, index, average_func)


@base.register_process('CDAT', 'sum', abstract=SUM_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def sum(self, context, index):
    def sum_func(data, axes):
        for axis in axes:
            axis_index = data.getAxisIndex(axis)

            if axis_index == -1:
                raise WPSError('Unknown axis {!s}', axis)

            data = MV2.sum(data, axis=axis_index)

        return data

    return process_data(self, context, index, sum_func)


@base.register_process('CDAT', 'max', abstract=MAX_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def max(self, context, index):
    def max_func(data, axes):
        for axis in axes:
            axis_index = data.getAxisIndex(axis)

            if axis_index == -1:
                raise WPSError('Unknown axis {!s}', axis)

            data = MV2.max(data, axis=axis_index)

        return data

    return process_data(self, context, index, max_func)


@base.register_process('CDAT', 'min', abstract=MIN_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def min(self, context, index):
    def min_func(data, axes):
        for axis in axes:
            axis_index = data.getAxisIndex(axis)

            if axis_index == -1:
                raise WPSError('Unknown axis {!s}', axis)

            data = MV2.min(data, axis=axis_index)

        return data

    return process_data(self, context, index, min_func)
