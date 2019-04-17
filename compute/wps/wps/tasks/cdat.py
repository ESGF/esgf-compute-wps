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
from distributed.scheduler import KilledWorker

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
    """ Formats a dimension.

    Formats a cwt.Dimension to either a slice or tuple. A slice denotes that the axis
    will not need to be mapped, whereas a tuple will need mapping.

    Args:
        dim: A cwt.Dimension to format.

    Returns:
        A slice or tuple.
    """

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
    """ Converts a domain to a dict.

    Args:
        domain: A cwt.Domain to convert.

    Returns:
        A dict mapping dimension names to the formatted dimension value.
    """
    if domain is None:
        return {}

    return dict((x, format_dimension(y)) for x, y in domain.dimensions.items())


def check_access(context, input):
    """ Check whether an input requires a certificate to access.

    Since its unknown whether an input requires an ESGF cert we first check access
    without one. If this fails we check access with a certificate. If this still
    fails then it's assumed that something has gone wrong.

    Args:
        context: A wps.context.OperationContext.
        input: A cwt.Variable to check.

    Returns:
        True if a certificate is required to access the file, False if one
        is not needed.

    Raises:
        cwt.WPSError if the file is unaccessible.
    """
    if not context.check_access(input):
        cert_path = credentials.load_certificate(context.user)

        if not context.check_access(input, cert_path):
            raise WPSError('Failed to access input')
        else:
            return True

    return False


def write_certificate(cert):
    """ Writes certificate and dodsrc.

    Writes SSL cert/key and dodsrc to current directory. This enables any
    netCDF library to access ESGF protected data.

    Args:
        cert: A str containing all certificates/keys.
    """
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
    """ Maps the dimensions of a domain to slices.

    Args:
        input: A cwt.Variable that's used to map the domain dimensions to the axes
            available in the source.
        domain: A cwt.Domain whos dimensions will be mapped onto the axes of file
            represented by input.
        cert: A str containing SSL cert/key for accessing data on ESGF data nodes.

    Returns:
        An OrderedDict and tuple. The OrderedDict maps axis names to their slice
        representation in the file. The tuple contains the units and first value
        of the time axis, which may be used when a variable spans multiple files
        i.e. an aggregation.

    """
    # Write the certificate it's required.
    if cert is not None:
        write_certificate(cert)

    import cdms2 # noqa

    axis_data = OrderedDict()

    ordering = None

    with cdms2.open(input.uri) as infile:
        # Iterate over all axes of the variable.
        for axis in infile[input.var_name].getAxisList():
            # Store the units and first value of the time axis.
            if axis.isTime():
                ordering = (axis.units, axis[0])

            if axis.id in domain:
                # Slices to not need to be mapped
                if isinstance(domain[axis.id], slice):
                    axis_data[axis.id] = domain[axis.id]
                else:
                    # Convert FileAxis to TransientAxis so values may be mapped.
                    axis_clone = axis.clone()
                    try:
                        # Map the first two values, ignoring the step.
                        interval = axis_clone.mapInterval(domain[axis.id][:2])
                    except TypeError:
                        # TypeError usually means the file is not included in the domain.
                        axis_data[axis.id] = None
                    else:
                        # Create slice from the mapped values and the original step value.
                        axis_data[axis.id] = slice(interval[0], interval[1], domain[axis.id][2])
            else:
                # If axis is not in domain just set slice to None value
                axis_data[axis.id] = slice(None, None, None)

    return axis_data, ordering


def slice_to_ijk(a, asel):
    """ Convert slice to tuple.

    Args:
        a: A cdms2.TransientAxis, used to get the length when stop is None.
        asel: A slice that will be converted.

    Returns:
        A tuple contaning the start, stop and step values.
    """
    i = asel.start or 0
    j = asel.stop or len(a)
    k = asel.step or 1
    return i, j, k


def merge_variables(context, data, domain):
    """ Merges multiple variables.

    Merge variables that are split across many files i.e. aggregations. All attributes are
    gathered from the first file. The axis and variable data are returned from the first
    file unless the axis is a time axis or if the variable contains the time axes. If the
    later is true then we collect the axis or variable from all the inputs and concatenate
    them. If the variable is the one being processed then it's not retrieved but replaced by
    the 'data' argument. See the example below.

    Variable: tas

    File1:
        Axes:
            time, lat, lon
        Vars:
            tas, lat_bnds, lon_bnds, time_bnds

    File2:
        Axes:
            time, lat, lon
        Vars:
            tas, lat_bnds, lon_bnds, time_bnds

    Output:
        Axes:
            (File1_time+File2_time), lat, lon
        Vars:
            tas (replaced by data), lat_bnds, lon_bnds, (File1_time_bnds+File2_time_bnds)

    Args:
        context: A wps.context.OperationContext that contains the varibale definitions.
        data: A dask.Array containing the variable.
        domain: A cwt.Domain that has been mapped over the file.

    Returns:
        Three dicts; the first contains the global attributes, taken from the first file.
        The second is a dict mapping axis names to a dict containing the axis attributes
        and data. The third is a dict mapping varibale names to a dict containing the
        variable data and attributes.
    """
    vars = {}
    axes = {}
    gattrs = None

    for input in context.inputs:
        processed_time = False

        with cdms2.open(input.uri) as infile:
            # Grab global attributes from first file.
            if gattrs is None:
                gattrs = infile.attributes

            # Iterate over variables in file
            for v in infile.getVariables():
                for a in v.getAxisList():
                    if a.isTime() and not processed_time:
                        if a.id in axes:
                            axes[a.id]['data'].append(a.clone())
                        else:
                            axes[a.id] = {'data': [a.clone(), ], 'attrs': a.attributes.copy()}
                        processed_time = True
                    elif a.id not in axes:
                        asel = domain.get(a.id, slice(None, None, None))
                        i, j, k = slice_to_ijk(a, asel)
                        axes[a.id] = {'data': a.subAxis(i, j, k), 'attrs': a.attributes.copy()}

                if v.id == input.var_name:
                    if v.id not in vars:
                        vars[v.id] = {'data': data, 'attrs': v.attributes.copy()}
                else:
                    if v.getTime() is None:
                        if v.id not in vars:
                            vsel = dict((x.id, domain[x.id]) for x in v.getAxisList() if x.id in domain)
                            vars[v.id] = {'data': v(**vsel), 'attrs': v.attributes.copy()}
                    else:
                        if v.id in vars:
                            vars[v.id]['data'].append(v())
                        else:
                            vars[v.id] = {'data': [v(), ], 'attrs': v.attributes.copy()}

                if 'axes' not in vars[v.id]:
                    vars[v.id]['axes'] = [x.id for x in v.getAxisList()]

    # Concatenate time axis and grab subAxis.
    for x, y in list(axes.items()):
        if isinstance(y['data'], list):
            logger.info('Concatenating axis %r over %r segments', x, len(y['data']))

            axis_concat = MV2.axisConcatenate(y['data'], id=x, attributes=y['attrs'])

            i, j, k = slice_to_ijk(axis_concat, domain.get(x, slice(None, None, None)))

            y['data'] = axis_concat.subAxis(i, j, k)

    # Concatenate time_bnds variable and grab subRegion.
    for x, y in list(vars.items()):
        if isinstance(y['data'], list):
            logger.info('Concatenating variable %r over %r segments', x, len(y['data']))

            var_concat = MV2.concatenate(y['data'])

            vsel = [domain[x.id] for x in var_concat.getAxisList() if x.id in domain]

            y['data'] = var_concat.subRegion(*vsel)

    return gattrs, axes, vars


def output_with_attributes(var_name, gattrs, axes, vars):
    """ Create output with original attributes.

    We create an xarray.Dataset from the axis/variable data thats been collected.

    Args:
        var_name: A str variable name.
        gattrs: A dict containing the outputs global attributes.
        axes: A dict mapping axis name to a dict containing the axis data and attributes.
        vars: A dict mapping variable name to a dict contiaing the variable data and attributes.

    Returns:
        An xarray.Dataset.
    """
    xr_axes = {}
    xr_vars = {}

    # Create xarray.DataArray for all axes
    for name, y in list(axes.items()):
        xr_axes[name] = xr.DataArray(y['data'], name=name, dims=name, attrs=y['attrs'])

    # Create xarray.DataArray for all variables
    for name, var in list(vars.items()):
        coords = {}

        # Grab the xarray.DataArrays containing the axes in the varibale
        for a in var['axes']:
            coords[a] = xr_axes[a]

        xr_vars[name] = xr.DataArray(var['data'], name=name, dims=coords.keys(), coords=coords, attrs=var['attrs'])

        # Set the _FillValue to 1e20 as a default
        if name == var_name:
            xr_vars[name].attrs['_FillValue'] = 1e20

    return xr.Dataset(xr_vars, attrs=gattrs)


def update_shape(shape, chunk, index):
    """ Updates a shape value.

    Example:
        Inputs:
            shape: (3650, 192, 288)
            chunk: slice(110, 2100, 1)
            index: 0

        Result:
            (1990, 192, 288)

    Args:
        shape: A list containing the shape of each axis in a variable.
        chunk: A slice containing the new shape of an axis.
        index: An int denoting the axis to replace.

    Returns:
        A tuple containing the new shape.
    """
    # Find the new size
    diff = chunk.stop - chunk.start

    # Remove the old value
    shape.pop(index)

    # Replace with the new valur.
    shape.insert(index, diff)

    # Return a tuple.
    return tuple(shape)


def build_ingress(var, cert, chunk_shape):
    """ Build delay data ingress.

    Builds a dask.delayed function to retrieve a chunk of `var`, this
    is always over the time axis.

    Args:
        var: A cdms2.TransientVariable/cdms2.FileVariable.
        cert: A str SSL certificate/key to access ESGF data.
        chunk_shape: A tuple containing the shape of a single chunk.

    Returns:
        A dask.Array that combines the delayed funcations.
    """
    # Grab the URL of the file containing the variable.
    url = var.parent.id

    # Get the size of the time axis
    size = var.getTime().shape[0]

    # Get index of the time access, may not always be index 0.
    index = var.getAxisIndex('time')

    # Get the step value to generate chunks
    step = chunk_shape[index]

    chunks = [{'time': slice(x, min(x+step, size), 1)} for x in range(0, size, step)]

    # Create a single delayed function for each chunk. Create a dask.Array from the delayed
    # function.
    da_chunks = [da.from_delayed(dask.delayed(retrieve_chunk)(url, var.id, x, cert),
                                 update_shape(list(var.shape), x['time'], 0),
                                 var.dtype)
                 for x in chunks]

    # Combine all dask.Arrays along the time axis
    concat = da.concatenate(da_chunks, axis=0)

    return concat


def build_regrid(context, axes, vars, selector):
    """ Builds regridding dask graph.

    Builds a dask graph for regridding, updates the `axes` and `vars` input dicts.

    Args:
        context: A wps.context.OperationContext.
        axes: A dict mapping axis name to a dict containing the axis data and attributes.
        vars: A dict mapping variable name to a dict containing the variable data and attributes.
        selector: A dict mapping axis names to slices.
    """
    # Only create regridding if lat and lon are present.
    if context.is_regrid and 'lat' in selector and 'lon' in selector:
        context.job.update('Building regrid graph')

        # Create a dict mapping axis names to a tuple of the first and last value for each axis present in
        # the selector.
        new_selector = dict((x, (axes[x]['data'][0], axes[x]['data'][-1])) for x, y in list(selector.items()))

        # Creates a grid thats a subRegion defined by the `new_selector`
        grid, tool, method = context.regrid_context(new_selector)

        context.job.update('Setting target grid to {!r} using {!r} tool and {!r} method', grid.shape, tool, method)

        var_name = context.inputs[0].var_name

        data = vars[var_name]['data']

        # Convert the input dask.Array to an array of delayed functions.
        delayed = data.to_delayed().squeeze()

        # if the dask array contains only a single chunk then
        # to_delayed produces a flat array and zip fails since its not iterable
        if delayed.size == 1:
            delayed = delayed.reshape((1, ))

        # Create list of axis data that is present in the selector.
        axis_data = [x['data'] for x in axes.values() if x['data'].id in selector]

        # Creates a new dask.Array from a dask.Delayed function that regrids each chunk of data
        regrid = [da.from_delayed(dask.delayed(regrid_data)(x, axis_data, grid, tool, method),
                                  y.shape[:-2] + grid.shape,
                                  data.dtype)
                  for x, y in zip(delayed, data.blocks)]

        # Concatenate all regrided chunks then override the original dask.Array
        vars[var_name]['data'] = da.concatenate(regrid)

        context.job.update('Finished building regrid graph')

        # Copy all attributes from original lat axis to new lat axis taken from the new grid.
        lat = grid.getLatitude()
        old_lat = axes['lat']['data']
        for x, y in old_lat.attributes.items():
            setattr(lat, x, y)
        axes['lat']['data'] = lat

        # Copy all attributes from original lon axis to new lon axis taken from the new grid.
        lon = grid.getLongitude()
        old_lon = axes['lon']['data']
        for x, y in old_lon.attributes.items():
            setattr(lon, x, y)
        axes['lon']['data'] = lon


def combine_maps(maps, index):
    """ Combine maps along the `index` axis.

    Example:
        Inputs:
            maps: [
                {
                    'time': slice(50, 121, 1),
                    'lat': slice(0, 180, 1),
                },
                {
                    'time': slice(0, 190, 1),
                    'lat': slice(0, 180, 1),
                },
            ]

            index: 0

        Result:
            {
                'time': slice(0, 261, 1),
                'lat': slice(0, 180, 1),
            }


    Args:
        maps: A list of dicts mapping axis names to slices.
        index: An int value denoting the index thats being combined over.

    Returns:
        A dict with combined maps.
    """
    start = None
    stop = None
    step = None
    template = None

    for x in maps:
        # Grab the axis_slice being combined
        axis_slice = list(x.values())[index]

        if axis_slice is None:
            continue

        # Use the first map as a template
        if template is None:
            template = x.copy()

        if start is None:
            # First time being processed, set initial values
            start = axis_slice.start

            stop = axis_slice.stop

            step = axis_slice.step
        else:
            # Extend the stop by the size of the axis
            stop += axis_slice.stop - axis_slice.start

    # Find the key of the axis being combined
    key = list(template.keys())[index]

    # Update the axis with the new slice
    template[key] = slice(start, stop, step)

    return template


def build_subset(context, input, var, cert):
    """ Build a dask subset graph.

    Args:
        context: A wps.context.OperationContext.
        input: A cwt.Variable that will be subsetted.
        var: A cdms2.FileVariable describe by `input`.
        cert: A str SSL certificate/key used to access ESGF data.
    """
    context.job.update('Building input graph')

    # Convert cwt.Domain to dict mapping axis names to base types.
    domain = domain_to_dict(context.domain)

    logger.info('Translated domain to %r', domain)

    # Map domain to variable axes.
    map, _ = map_domain(input, domain)

    logger.info('Mapped domain to %r', map)

    # Determine a base chunk value.
    chunks = (100,) + var.shape[1:]

    logger.info('Setting chunk to %r', chunks)

    # Construct the input dask.Array. If certificates are not required
    # the Array is create from the cdms2.FileVariable otherwise we created
    # delayed functions so the certificate can be written local to the dask
    # worker enabling access to protected ESGF data.
    if cert is None:
        data = da.from_array(var, chunks=chunks)
    else:
        data = build_ingress(var, cert, chunks)

    logger.info('Build data ingress %r', data)

    # Create selector from the mapped slices.
    selector = tuple(map.values())

    logger.info('Subsetting with selector %r', selector)

    # Subset the dask.Array.
    subset_data = data[selector]

    logger.info('Subset %r', subset_data)

    context.job.update('Build input shape {!r}, chunksize {!r}', subset_data.shape, subset_data.chunksize)

    return subset_data, map


def process_single(context, process):
    """ Process a single variable.

    Initialize the cluster workers, create the dask graph then execute.

    Args:
        context: A cwt.context.OperationContext.
        process: A function can be used to apply addition operations the the dask.Array.

    Returns:
        An updated cwt.context.OperationContext.
    """
    init(context.user.id, settings.DASK_WORKERS)

    context.job.update('Initialized {!r} workers', settings.DASK_WORKERS)

    input = context.inputs[0]

    cert = context.user.auth.cert if check_access(context, input) else None

    context.job.update('Building compute graph')

    infile = None

    # Cannot use ContextManager otherwise dask exhibits odd behavior.
    try:
        infile = cdms2.open(input.uri)

        var = infile[input.var_name]

        logger.info('Opened file %r variable %r', var.parent.id, var.id)

        # Build the subset dask.Array.
        data, map = build_subset(context, input, var, cert)

        # Apply process function is present.
        if process is not None:
            data = process(context, data, var, map)

        context.job.update('Finished building compute graph')

        # Collect variable/axis data and attributes
        gattrs, axes, vars = merge_variables(context, data, map)

        # Build regrid graph if requested.
        build_regrid(context, axes, vars, map)

        context.job.update('Building output definition')

        # Build the output with original attributes
        dataset = output_with_attributes(context.inputs[0].var_name, gattrs, axes, vars)

        context.job.update('Finished building output definition')

        # Create the output directory
        context.output_path = context.gen_public_path()

        logger.info('Writing output to %r', context.output_path)

        context.job.update('Starting compute')

        # Execute the dask graph
        dataset.to_netcdf(context.output_path)

        context.job.update('Finished compute')
    except KilledWorker as e:
        logger.exception('Error talking to dask worker, work died %r', e)

        raise WPSError('Error talking to dask workers.')
    except Exception:
        raise
    finally:
        if infile is not None:
            infile.close()

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
    init(context.user.id, settings.DASK_WORKERS)

    # Convert domain to dict.
    domain = domain_to_dict(context.domain)

    logger.info('Translated domain to %r', domain)

    cert = context.user.auth.cert if check_access(context, context.inputs[0]) else None

    logger.info('Authorization required %r', cert is not None)

    # Map the domain over each input variable
    maps = [(x.uri,) + map_domain(x, domain) for x in context.inputs]

    logger.info('Domain maps %r', maps)

    # True if all units are unique.
    order_by_units = len(set(x[-1][0] for x in maps)) == len(context.inputs)
    # True if all first values are unique.
    order_by_first = len(set(x[-1][1] for x in maps)) == len(context.inputs)

    logger.info('Ordering by units %r first %r', order_by_units, order_by_first)

    if order_by_units:
        # Order the input maps by units of the time axis.
        ordered_inputs = sorted(maps, key=lambda x: x[-1][0])
    elif order_by_first:
        # Order the input maps by the first value in the time axis.
        ordered_inputs = sorted(maps, key=lambda x: x[-1][1])
    else:
        raise WPSError('Unable to determine the order of inputs')

    logger.info('Ordered maps %r', ordered_inputs)

    chunks = None

    data = []

    infiles = []

    # Cannot use nested ContextManagers, causes odd behavior in dask.
    try:
        # Build input dask graphs
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

        # Concatenated inputs over time axis.
        concat = da.concatenate(data, axis=0)

        logger.info('Concat inputs %r', concat)

        # Combind the maps over the time axis
        combined = combine_maps([x[1] for x in maps], 0)

        logger.info('Combined maps to %r', combined)

        # Create selector from combined slices
        selector = tuple(combined.values())

        # Take the subset of the concatenated inputs
        subset_data = concat[selector]

        logger.info('Subset concat to %r', subset_data)

        # Merge the variables over the time axis
        gattrs, axes, vars = merge_variables(context, subset_data, combined)

        # Build regrid graph if requested.
        build_regrid(context, axes, vars, combined)

        # Build output with original attributes
        dataset = output_with_attributes(context.inputs[0].var_name, gattrs, axes, vars)

        context.output_path = context.gen_public_path()

        logger.info('Writing output to %r', context.output_path)

        # Execute the dask graph.
        dataset.to_netcdf(context.output_path)
    except KilledWorker as e:
        logger.exception('Error talking to dask worker, work died %r', e)

        raise WPSError('Error talking to dask workers.')
    except Exception:
        raise
    finally:
        for infile in infiles:
            infile.close()

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
    init(context.user.id, settings.DASK_WORKERS)

    return context


@base.register_process('CDAT', 'sum', abstract=SUM_ABSTRACT, metadata={'inputs': '1'})
@base.cwt_shared_task()
def sum_func(self, context):
    """ Sum Celery task.
    """
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
    """ Max Celery task.
    """
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
    """ Min Celery task.
    """
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
