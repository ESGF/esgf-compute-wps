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
from dask.distributed import Client
from distributed.protocol.serialize import register_serialization

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
op1.domain = 'd0'
op1.inputs = ['v0', 'v0.1']
op1._identifier = 'CDAT.aggregate'
op1.parameters['gridder'] = cwt.Gridder(grid='gaussian~32')

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
                    vars[v.id]['axes'] = [x.id for x in v.getAxisList()]

    # Concatenate time axis
    for x, y in list(axes.items()):
        if isinstance(y['data'], list):
            axis_concat = MV2.axisConcatenate(y['data'], id=x, attributes=y['attrs'])

            i, j, k = slice_to_ijk(axis_concat, domain.get(x, slice(None, None, None)))

            y['data'] = axis_concat.subAxis(i, j, k)

    # Concatenate time_bnds variable
    for x, y in list(vars.items()):
        if isinstance(y['data'], list):
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


def regrid_data(data, axes, grid, tool, method):
    import cdms2

    # Subset time to just fit, don't care if its the correct range
    axes[0] = axes[0].subAxis(0, data.shape[0], 1)

    var = cdms2.createVariable(data, axes=axes)

    shape = var.shape

    data = var.regrid(grid, regridTool=tool, regridMethod=method)

    logger.info('Regrid %r -> %r', shape, data.shape)

    return data


def build_regrid(context, axes, vars, selector):
    if context.is_regrid:
        new_selector = dict((x, (axes[x]['data'][0], axes[x]['data'][-1])) for x, y in list(selector.items()))

        grid, tool, method = context.regrid_context(new_selector)

        var_name = context.inputs[0].var_name

        data = vars[var_name]['data']

        delayed = data.to_delayed().squeeze()

        axis_data = [x['data'] for x in axes.values() if x['data'].id in selector]

        regrid = [da.from_delayed(dask.delayed(regrid_data)(x, axis_data, grid, tool, method),
                                  y.shape[:-2] + grid.shape,
                                  data.dtype)
                  for x, y in zip(delayed, data.blocks)]

        vars[var_name]['data'] = da.concatenate(regrid)

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


def build_subset(context):
    domain = domain_to_dict(context.domain)

    logger.info('Translated domain to %r', domain)

    input = context.inputs[0]

    cert = context.user.auth.cert if check_access(context, input) else None

    map, _ = map_domain(input, domain)

    logger.info('Mapped domain to %r', map)

    with cdms2.open(input.uri) as infile:
        var = infile[input.var_name]

        chunks = (100,) + var.shape[1:]

        logger.info('Setting chunk to %r', chunks)

        if cert is None:
            data = da.from_array(var, chunks=chunks)
        else:
            data = build_ingress(var, cert, chunks)

    selector = tuple(map.values())

    logger.info('Subsetting variable %r with selector %r', input.var_name, selector)

    subset_data = data[selector]

    return subset_data, map


@base.register_process('CDAT', 'subset', abstract=SUBSET_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def subset_func(self, context):
    subset_data, map = build_subset(context)

    gattrs, axes, vars = merge_variables(context, subset_data, map)

    build_regrid(context, axes, vars, map)

    dataset = output_with_attributes(context.inputs[0].var_name, gattrs, axes, vars)

    context.output_path = context.gen_public_path()

    logger.info('Writing output to %r', context.output_path)

    dataset.to_netcdf(context.output_path)

    return context


@base.register_process('CDAT', 'aggregate', abstract=AGGREGATE_ABSTRACT, metadata={'inputs': '*'})
@base.cwt_shared_task()
def aggregate_func(self, context):
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

    for input in context.inputs:
        with cdms2.open(input.uri) as infile:
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

    return context


@base.register_process('CDAT', 'regrid', abstract=REGRID_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def regrid_func(self, context):
    # Ensure gridder is provided
    context.operation.get_parameter('gridder', True)

    return subset_func(context)


@base.register_process('CDAT', 'average', abstract=AVERAGE_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def average_func(self, context):
    return context


@base.register_process('CDAT', 'sum', abstract=SUM_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def sum_func(self, context):
    return context


@base.register_process('CDAT', 'max', abstract=MAX_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def max_func(self, context):
    return context


@base.register_process('CDAT', 'min', abstract=MIN_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def min_func(self, context):
    return context


def serialize_transient_axis(axis):
    axis_data = axis[:]
    bounds = axis.getBounds()

    header = {
        'id': axis.id,
        'axis': {
            'shape': axis_data.shape,
            'dtype': axis_data.dtype.name,
        },
        'bounds': {
            'shape': bounds.shape,
            'dtype': bounds.dtype.name,
        },
        'units': axis.units
    }
    data = [axis_data.tobytes(), bounds.tobytes()]
    return header, data


def deserialize_transient_axis(header, frames):
    import cdms2
    import numpy as np
    axis_data = np.frombuffer(frames[0], dtype=header['axis']['dtype'])
    axis_data = axis_data.reshape(header['axis']['shape'])

    bounds = np.frombuffer(frames[1], dtype=header['bounds']['dtype'])
    bounds = bounds.reshape(header['bounds']['shape'])

    axis = cdms2.createAxis(axis_data, bounds=bounds, id=header['id'])
    axis.units = header['units']

    return axis


register_serialization(cdms2.axis.TransientAxis, serialize_transient_axis, deserialize_transient_axis)
