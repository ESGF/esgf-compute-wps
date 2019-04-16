#! /usr/bin/env python

from collections import OrderedDict

import cdms2
import cwt
import dask
import dask.array as da
import xarray as xr
from cdms2 import MV2
from celery.utils.log import get_task_logger
from django.conf import settings
from dask.distributed import LocalCluster

from wps import WPSError
from wps.tasks import base
from wps.tasks import credentials
from wps.tasks.dask_serialize import regrid_data
from wps.tasks.dask_serialize import retrieve_chunk
from cwt_kubernetes.cluster import Cluster
from cwt_kubernetes.cluster_manager import ClusterManager

logger = get_task_logger('wps.tasks.cdat')

BACKEND = 'CDAT'


def init(user_id, n_workers):
    if settings.DEBUG:
        cluster = LocalCluster(n_workers=2, threads_per_worker=2)

        class LocalProvisioner(object):
            def __init__(self, local):
                self.local = local

            def get_pods(self):
                class Inner(object):
                    def __init__(self, items):
                        self.items = items

                return Inner(self.local.workers)

        manager = ClusterManager(cluster, LocalProvisioner(cluster))
    else:
        cluster = Cluster.from_yaml(settings.DASK_KUBE_NAMESPACE, '/etc/config/dask/worker-spec.yml')

        manager = ClusterManager(settings.DASK_SCHEDULER, cluster)

        labels = {
            'user': str(user_id),
        }

        manager.scale_up_workers(n_workers, labels)

    return manager, cluster


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
        else:
            return True

    return False


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


def slice_to_ijk(a, asel):
    i = asel.start or 0
    j = asel.stop or len(a)
    k = asel.step or 1
    return i, j, k


def merge_variables(context, data, domain):
    vars = {}
    axes = {}
    gattrs = None

    for input in context.inputs:
        with cdms2.open(input.uri) as infile:
            if gattrs is None:
                gattrs = infile.attributes

            for v in infile.getVariables():
                if v.id == input.var_name:
                    for a in v.getAxisList():
                        if a.isTime():
                            if a.id in axes:
                                axes[a.id]['data'].append(a.clone())
                            else:
                                axes[a.id] = {'data': [a.clone(), ], 'attrs': a.attributes}
                        elif a.id not in axes:
                            asel = domain.get(a.id, slice(None, None, None))

                            i, j, k = slice_to_ijk(a, asel)

                            axes[a.id] = {'data': a.subAxis(i, j, k), 'attrs': a.attributes}

                if v.id == input.var_name:
                    if v.id not in vars:
                        vars[v.id] = {'data': data, 'attrs': v.attributes}
                else:
                    if v.getTime() is not None:
                        if v.id in vars:
                            vars[v.id]['data'].append(v())
                        else:
                            vars[v.id] = {'data': [v(), ], 'attrs': v.attributes}
                    elif v.id not in vars:
                        vsel = dict((x.id, domain[x.id]) for x in v.getAxisList() if x.id in domain)
                        vars[v.id] = {'data': v(**vsel), 'attrs': v.attributes}

                if 'axes' not in vars[v.id]:
                    vars[v.id]['axes'] = [x.id for x in v.getAxisList() if x.id in domain]

    # Concatenate time axis
    for x, y in list(axes.items()):
        if isinstance(y['data'], list):
            logger.info('Concatenating axis %r over %r segments', x, len(y['data']))

            axis_concat = MV2.axisConcatenate(y['data'], id=x, attributes=y['attrs'])

            i, j, k = slice_to_ijk(axis_concat, domain.get(x, slice(None, None, None)))

            y['data'] = axis_concat.subAxis(i, j, k)

    # Concatenate time_bnds variable
    for x, y in list(vars.items()):
        if isinstance(y['data'], list):
            logger.info('Concatenating variable %r over %r segments', x, len(y['data']))

            var_concat = MV2.concatenate(y['data'])

            vsel = [domain[x.id] for x in var_concat.getAxisList() if x.id in domain]

            y['data'] = var_concat.subRegion(*vsel)

    return gattrs, axes, vars


def output_with_attributes(var_name, gattrs, axes, vars):
    xr_axes = {}
    xr_vars = {}

    for name, y in list(axes.items()):
        xr_axes[name] = xr.DataArray(y['data'], name=name, dims=name, attrs=y['attrs'])

    for name, var in list(vars.items()):
        if name == var_name:
            coords = {}

            for a in var['axes']:
                coords[a] = xr_axes[a]

            xr_vars[name] = xr.DataArray(var['data'], name=name, dims=coords.keys(), coords=coords, attrs=var['attrs'])

            if name == var_name:
                xr_vars[name].attrs['_FillValue'] = 1e20

    return xr.Dataset(xr_vars, attrs=gattrs)


def update_shape(shape, chunk, index):
    diff = chunk.stop - chunk.start

    shape.pop(index)

    shape.insert(index, diff)

    return tuple(shape)


def build_ingress(var, cert, chunk_shape):
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


def build_regrid(context, axes, vars, selector):
    if context.is_regrid and 'lat' in selector and 'lon' in selector:
        context.job.update('Building regrid graph')

        new_selector = dict((x, (axes[x]['data'][0], axes[x]['data'][-1])) for x, y in list(selector.items()))

        grid, tool, method = context.regrid_context(new_selector)

        context.job.update('Setting target grid to {!r} using {!r} tool and {!r} method', grid.shape, tool, method)

        var_name = context.inputs[0].var_name

        data = vars[var_name]['data']

        delayed = data.to_delayed().squeeze()

        # if the dask array contains only a single chunk then
        # to_delayed produces a flat array and zip fails since its not iterable
        if delayed.size == 1:
            delayed = delayed.reshape((1, ))

        axis_data = [x['data'] for x in axes.values() if x['data'].id in selector]

        regrid = [da.from_delayed(dask.delayed(regrid_data)(x, axis_data, grid, tool, method),
                                  y.shape[:-2] + grid.shape,
                                  data.dtype)
                  for x, y in zip(delayed, data.blocks)]

        vars[var_name]['data'] = da.concatenate(regrid)

        context.job.update('Finished building regrid graph')

        lat = grid.getLatitude()
        old_lat = axes['lat']['data']
        for x, y in old_lat.attributes.items():
            setattr(lat, x, y)
        axes['lat']['data'] = lat

        lon = grid.getLongitude()
        old_lon = axes['lon']['data']
        for x, y in old_lon.attributes.items():
            setattr(lon, x, y)
        axes['lon']['data'] = lon


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


def build_subset(context, input, var, cert):
    context.job.update('Building input graph')

    domain = domain_to_dict(context.domain)

    logger.info('Translated domain to %r', domain)

    map, _ = map_domain(input, domain)

    logger.info('Mapped domain to %r', map)

    chunks = (100,) + var.shape[1:]

    logger.info('Setting chunk to %r', chunks)

    if cert is None:
        data = da.from_array(var, chunks=chunks)
    else:
        data = build_ingress(var, cert, chunks)

    logger.info('Build data ingress %r', data)

    selector = tuple(map.values())

    logger.info('Subsetting with selector %r', selector)

    subset_data = data[selector]

    logger.info('Subset %r', subset_data)

    context.job.update('Build input shape {!r}, chunksize {!r}', subset_data.shape, subset_data.chunksize)

    return subset_data, map


def process_single(context, process):
    init(context.user.id, settings.DASK_WORKERS)

    context.job.update('Initialized {!r} workers', settings.DASK_WORKERS)

    input = context.inputs[0]

    cert = context.user.auth.cert if check_access(context, input) else None

    context.job.update('Building compute graph')

    infile = None

    try:
        infile = cdms2.open(input.uri)

        var = infile[input.var_name]

        logger.info('Opened file %r variable %r', var.parent.id, var.id)

        data, map = build_subset(context, input, var, cert)

        if process is not None:
            data = process(context, data, var, map)

        context.job.update('Finished building compute graph')

        gattrs, axes, vars = merge_variables(context, data, map)

        build_regrid(context, axes, vars, map)

        context.job.update('Building output definition')

        dataset = output_with_attributes(context.inputs[0].var_name, gattrs, axes, vars)

        context.job.update('Finished building output definition')

        context.output_path = context.gen_public_path()

        logger.info('Writing output to %r', context.output_path)

        context.job.update('Starting compute')

        dataset.to_netcdf(context.output_path)

        context.job.update('Finished compute')
    except Exception:
        raise
    finally:
        if infile is not None:
            infile.close()

    return context


@base.register_process('CDAT', 'subset', abstract=SUBSET_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def subset_func(self, context):
    return process_single(context, None)


@base.register_process('CDAT', 'aggregate', abstract=AGGREGATE_ABSTRACT, metadata={'inputs': '*'})
@base.cwt_shared_task()
def aggregate_func(self, context):
    init(context.user.id, settings.DASK_WORKERS)

    domain = domain_to_dict(context.domain)

    logger.info('Translated domain to %r', domain)

    cert = context.user.auth.cert if check_access(context, context.inputs[0]) else None

    logger.info('Authorization required %r', cert is not None)

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

    logger.info('Ordered maps %r', ordered_inputs)

    chunks = None

    data = []

    infiles = []

    try:
        for input in context.inputs:
            infile = cdms2.open(input.uri)

            infiles.append(infile)

            var = infile[input.var_name]

            if chunks is None:
                chunks = (100,) + var.shape[1:]

            if cert is None:
                data.append(da.from_array(var, chunks=chunks))
            else:
                data.append(build_ingress(var, cert, chunks))

        concat = da.concatenate(data, axis=0)

        logger.info('Concat inputs %r', concat)

        combined = combine_maps([x[1] for x in maps], 0)

        logger.info('Combined maps to %r', combined)

        selector = tuple(combined.values())

        subset_data = concat[selector]

        logger.info('Subset concat to %r', subset_data)

        gattrs, axes, vars = merge_variables(context, subset_data, combined)

        build_regrid(context, axes, vars, combined)

        dataset = output_with_attributes(context.inputs[0].var_name, gattrs, axes, vars)

        context.output_path = context.gen_public_path()

        logger.info('Writing output to %r', context.output_path)

        dataset.to_netcdf(context.output_path)
    except Exception:
        raise
    finally:
        for infile in infiles:
            infile.close()

    return context


@base.register_process('CDAT', 'regrid', abstract=REGRID_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def regrid_func(self, context):
    # Ensure gridder is provided
    context.operation.get_parameter('gridder', True)

    return subset_func(context)


# @base.register_process('CDAT', 'average', abstract=AVERAGE_ABSTRACT, metadata={'inputs': '1'})
# @base.cwt_shared_task()
def average_func(self, context):
    init(context.user.id, settings.DASK_WORKERS)

    return context


@base.register_process('CDAT', 'sum', abstract=SUM_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def sum_func(self, context):
    def process(context, data, var, map):
        axes = context.operation.get_parameter('axes', True)

        axes_index = []

        for x in axes.values:
            index = var.getAxisIndex(x)

            if index == -1:
                raise WPSError('Failed to find axis %r in source file', x)

            axes_index.append(index)

            try:
                map.pop(x)
            except KeyError:
                raise Exception('Axis %r not present in mapped domain', x)

        return data.sum(axis=tuple(axes_index))

    return process_single(context, process)


@base.register_process('CDAT', 'max', abstract=MAX_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def max_func(self, context):
    def process(context, data, var, map):
        axes = context.operation.get_parameter('axes', True)

        axes_index = []

        for x in axes.values:
            index = var.getAxisIndex(x)

            if index == -1:
                raise WPSError('Failed to find axis %r in source file', x)

            axes_index.append(index)

            try:
                map.pop(x)
            except KeyError:
                raise Exception('Axis %r not present in mapped domain', x)

        return data.max(axis=tuple(axes_index))

    return process_single(context, process)


@base.register_process('CDAT', 'min', abstract=MIN_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def min_func(self, context):
    def process(context, data, var, map):
        axes = context.operation.get_parameter('axes', True)

        axes_index = []

        for x in axes.values:
            index = var.getAxisIndex(x)

            if index == -1:
                raise WPSError('Failed to find axis %r in source file', x)

            axes_index.append(index)

            try:
                map.pop(x)
            except KeyError:
                raise Exception('Axis %r not present in mapped domain', x)

        return data.min(axis=tuple(axes_index))

    return process_single(context, process)
