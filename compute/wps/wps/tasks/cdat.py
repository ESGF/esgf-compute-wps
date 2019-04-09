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

variable = {
    'v0': cwt.Variable('http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/CMIP/NCAR/CESM2/amip/r1i1p1f1/day/tas/gn/v20190218/tas_day_CESM2_amip_r1i1p1f1_gn_19500101-19591231.nc', 'tas', name='v0'),
    'v0.1': cwt.Variable('http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/CMIP/NCAR/CESM2/amip/r1i1p1f1/day/tas/gn/v20190218/tas_day_CESM2_amip_r1i1p1f1_gn_19600101-19691231.nc', 'tas', name='v0.1'),
    'v1': cwt.Variable('http://aims3.llnl.gov/thredds/dodsC/cmip5_css02_data/cmip5/output1/CMCC/CMCC-CM/historical/day/atmos/day/r1i1p1/clt/1/clt_day_CMCC-CM_historical_r1i1p1_19500101-19501231.nc', 'clt', name='v1'),
}

domain = {
    'd0': cwt.Domain([cwt.Dimension('time', 714968.0, 715044.0),
                      cwt.Dimension('lat', -90, 0)], name='d0'),
    'd0.1': cwt.Domain([cwt.Dimension('time', 714968.0, 715034.0),
                      cwt.Dimension('lat', -90, 0)], name='d0.1'),
    'd1': cwt.Domain([cwt.Dimension('time', 50, 150),
                      cwt.Dimension('lat', -90, 0),
                      cwt.Dimension('lon', 0, 90)], name='d1'),
}

op1 = cwt.Process(name='op1')
op1.domain = 'd0.1'
op1.inputs = ['v0', 'v0.1']
op1._identifier = 'CDAT.aggregate'
#op1.parameters['gridder'] = cwt.Gridder(grid='gaussian~32')

operation = {
    'op1': op1,
}

c = OperationContext.from_data_inputs('CDAT.aggregate', variable, domain, operation)

import mock
from wps import models

c.user = models.User.objects.get(pk=1)

c.job = mock.MagicMock()
c.job.id = 1

from dask.distributed import LocalCluster

cluster = LocalCluster(n_workers=2, threads_per_worker=2)

client = Client(cluster)

# client = Client(os.environ['DASK_SCHEDULER']) # noqa

# cluster = Cluster.from_yaml('default', 'worker-spec.yml')


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


def check_access(context, input):
    if not context.check_access(input):
        cert_path = credentials.load_certificate(context.user)

        if not context.check_access(input, cert_path):
            raise WPSError('Failed to access input')

    return True


def write_certificate(cert):
    with open('cert.pem', 'w') as outfile:
        outfile.write(cert)

    import os

    cert_path = os.path.join(os.getcwd(), 'cert.pem')

    with open('.dodsrc', 'w') as outfile:
        outfile.write('HTTP.COOKIEJAR=.dods_cookies\n')
        outfile.write('HTTP.SSL.CERTIFICATE={}\n'.format(cert_path))
        outfile.write('HTTP.SSL.KEY={}\n'.format(cert_path))
        outfile.write('HTTP.SSL.VERIFY=0\n')


def map_domain(input, domain, cert=None):
    if cert is not None:
        write_certificate(cert)

    import cdms2 # noqa

    axis_data = OrderedDict()

    ordering = None

    with cdms2.open(input.uri) as infile:
        for axis in infile[input.var_name].getAxisList():
            if axis.isTime():
                ordering = (axis.units, axis[0])

            if axis.id in domain:
                if isinstance(domain[axis.id], slice):
                    axis_data[axis.id] = domain[axis.id]
                else:
                    axis_clone = axis.clone()
                    try:
                        interval = axis_clone.mapInterval(domain[axis.id][:2])
                    except TypeError:
                        axis_data[axis.id] = None
                    else:
                        axis_data[axis.id] = slice(interval[0], interval[1], domain[axis.id][2])
            else:
                axis_data[axis.id] = slice(None, None, None)

    return axis_data, ordering


def output_with_attributes(urls, subset_data, domain, var_name):
    vars = {}
    vars_axes = {}
    axes = {}
    attrs = {}
    gattrs = None

    for url in urls:
        with cdms2.open(url) as infile:
            if gattrs is None:
                gattrs = infile.attributes

            for v in infile.getVariables():
                selector = {}

                if v.id not in attrs:
                    attrs[v.id] = v.attributes

                for a in v.getAxisList():
                    if v.id in vars_axes:
                        vars_axes[v.id].append(a.id)
                    else:
                        vars_axes[v.id] = [a.id, ]

                    if a.id not in attrs:
                        attrs[a.id] = a.attributes

                    if a.isTime():
                        if a.id in axes:
                            axes[a.id].append(a.clone())
                        else:
                            axes[a.id] = [a.clone(), ]
                    elif a.id not in axes:
                        axes[a.id] = a.clone()

                    if a.id in domain:
                        selector[a.id] = domain[a.id]

                if v.id == var_name:
                    if v.id not in vars:
                        vars[v.id] = subset_data
                else:
                    if v.getTime() is not None:
                        if v.id in vars:
                            vars[v.id].append(v())
                        else:
                            vars[v.id] = [v(), ]
                    elif v.id not in vars:
                        vars[v.id] = v(**selector)

    for x, y in list(axes.items()):
        if isinstance(y, list):
            selector = domain[x]

            axes[x] = MV2.axisConcatenate(y, id=x)

            if selector is not None:
                i = selector.start or 0
                j = selector.stop or len(axes[x])
                k = selector.step or 1

                axes[x] = axes[x].subAxis(i, j, k)
        else:
            if x in domain:
                selector = domain[x]

                if selector is not None:
                    i = selector.start or 0
                    j = selector.stop or len(axes[x])
                    k = selector.step or 1

                    axes[x] = axes[x].subAxis(i, j, k)

    for x, y in list(vars.items()):
        if isinstance(y, list):
            vars[x] = MV2.concatenate(y)

            selector = []

            for a in vars[x].getAxisList():
                if a.id in domain:
                    selector.append(domain[a.id])

            vars[x] = vars[x].subRegion(*selector)

    xr_axes = {}
    xr_vars = {}

    for x, y in list(axes.items()):
        xr_axes[x] = xr.DataArray(y, name=x, dims=x, attrs=attrs[x])

    for x, y in list(vars.items()):
        coords = {}

        for a in vars_axes[x]:
            coords[a] = xr_axes[a]

        xr_vars[x] = xr.DataArray(y, name=x, dims=coords.keys(), coords=coords, attrs=attrs[x])

        if x == var_name:
            xr_vars[x].attrs['_FillValue'] = 1e20

    return xr.Dataset(xr_vars, attrs=gattrs)


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


def regrid_data(url, var_name, data, grid, tool, method, cert):
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

    import cdms2

    with cdms2.open(url) as infile:
        var = infile[var_name]

        shape = data.shape

        axes = []

        for i, x in enumerate(var.getAxisList()):
            axes.append(x.subAxis(0, shape[i], 1))

        var = cdms2.createVariable(data, axes=axes)

    data = var.regrid(grid, regridTool=tool, regridMethod=method)

    logger.info('Regrid %r -> %r', shape, data.shape)

    return data


def axis_indices_to_values(var, name, value):
    axis_index = var.getAxisIndex(name)

    axis = var.getAxis(axis_index)

    start = axis[0] if value.start is None else axis[value.start]

    stop = axis[-1] if value.stop is None else axis[value.stop]

    return (start, stop)


def construct_regrid(var, context, data, selector):
    if context.is_regrid:
        grid, tool, method = context.regrid_context(selector)

        new_selector = dict((x, axis_indices_to_values(var, x, y)) for x, y in list(selector.items()))

        grid = context.subset_grid(grid, new_selector)

        delayed = data.to_delayed().squeeze()

        # This may not be a solid way to determine the new shape
        new_shape = data.chunksize[:-2] + grid.shape

        updated_selector = selector.copy()

        if 'lat' in selector:
            updated_selector['lat'] = slice(0, grid.getLatitude().shape[0], selector['lat'].step)

        if 'lon' in selector:
            updated_selector['lon'] = slice(0, grid.getLongitude().shape[0], selector['lon'].step)

        regrid = [da.from_delayed(dask.delayed(regrid_data)(var.parent.id, var.id, x, grid, tool,
                                                            method, context.user.auth.cert),
                                  new_shape,
                                  data.dtype)
                  for x in delayed]

        return da.concatenate(regrid), updated_selector

    return data, selector


def combine_maps(maps, index):
    """ Combine maps.
    """
    start = None
    stop = None
    step = None
    template = None

    for x in maps:
        axis_slice = list(x.values())[index]

        if axis_slice is None:
            continue

        if template is None:
            template = x.copy()

        if start is None:
            start = axis_slice.start

            stop = axis_slice.stop

            step = axis_slice.step
        else:
            stop += axis_slice.stop - axis_slice.start

    key = list(template.keys())[index]

    template[key] = slice(start, stop, step)

    return template


@base.register_process('CDAT', 'subset', abstract=SUBSET_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def subset_func(self, context):
    """ Subsetting data.
    """
    domain = domain_to_dict(context.domain)

    logger.info('Translated domain to %r', domain)

    input = context.inputs[0]

    cert = context.user.auth.cert if check_access(context, input) else None

    map, _ = map_domain(input, domain)

    logger.info('Mapped domain to %r', map)

    infile = cdms2.open(input.uri)

    var = infile[input.var_name]

    chunks = (100,) + var.shape[1:]

    logger.info('Setting chunk to %r', chunks)

    if cert is None:
        data = da.from_array(var, chunks=chunks)
    else:
        data = construct_ingress(var, cert, chunks)

    selector = tuple(map.values())

    logger.info('Subsetting variable %r with selector %r', input.var_name, selector)

    subset_data = data[selector]

    subset_data, map = construct_regrid(var, context, subset_data, map)

    dataset = output_with_attributes([input.uri, ], subset_data, map, input.var_name)

    context.output_path = context.gen_public_path()

    logger.info('Writing output to %r', context.output_path)

    dataset.to_netcdf(context.output_path)

    infile.close()

    return context


@base.register_process('CDAT', 'aggregate', abstract=AGGREGATE_ABSTRACT, metadata={'inputs': '*'})
@base.cwt_shared_task()
def aggregate_func(self, context):
    """ Aggregating data.
    """
    domain = domain_to_dict(context.domain)

    logger.info('Translated domain to %r', domain)

    cert = context.user.auth.cert if check_access(context, context.inputs[0]) else None

    maps = [(x.uri,) + map_domain(x, domain) for x in context.inputs]

    logger.info('Domain maps %r', maps)

    order_by_units = len(set(x[-1][0] for x in maps)) == len(context.inputs)
    order_by_first = len(set(x[-1][1] for x in maps)) == len(context.inputs)

    logger.info('Ordering by units %r first %r', order_by_units, order_by_first)

    if order_by_units:
        ordered_inputs = sorted(maps, key=lambda x: x[-1][0])
    elif order_by_first:
        ordered_inputs = sorted(maps, key=lambda x: x[-1][1])
    else:
        raise WPSError('Unable to determine the order of inputs')

    logger.info('Ordered inputs %r', ordered_inputs)

    chunks = None

    data = []

    for input in context.inputs:
        with cdms2.open(input.uri) as infile:
            var = infile[input.var_name]

            if chunks is None:
                chunks = (100,) + var.shape[1:]

            if cert is None:
                data.append(da.from_array(var, chunks=chunks))
            else:
                data.append(construct_ingress(var, cert, chunks))

    concat = da.concatenate(data, axis=0)

    combined = combine_maps([x[1] for x in maps], 0)

    selector = tuple(combined.values())

    subset_data = concat[selector]

    # TODO Regrid data

    dataset = output_with_attributes([x.uri for x in context.inputs], subset_data, combined, context.inputs[0].var_name)

    context.output_path = context.gen_public_path()

    logger.info('Writing output to %r', context.output_path)

    dataset.to_netcdf(context.output_path)

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
