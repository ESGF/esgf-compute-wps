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
from dask.distributed import LocalCluster
from distributed.diagnostics.progressbar import ProgressBar
from distributed.utils import LoopRunner
from distributed.client import futures_of
from tornado.ioloop import IOLoop

from wps import AccessError
from wps import WPSError
from wps.tasks import base
from wps.tasks import credentials
from wps.tasks.dask_serialize import regrid_data
from wps.tasks.dask_serialize import retrieve_chunk
from cwt_kubernetes.cluster import Cluster
from cwt_kubernetes.cluster_manager import ClusterManager

logger = get_task_logger('wps.tasks.cdat')

BACKEND = 'CDAT'


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
    raise WPSError('Workflows are disabled')

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

    output = dict((x, format_dimension(y)) for x, y in domain.dimensions.items())

    logger.info('Converted domain to %r', output)

    return output


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


def map_domain(var, domain, cert=None):
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
    axis_data = OrderedDict()

    ordering = None

    logger.info('Mapping domain to file %r', var.parent.id)

    for axis in var.getAxisList():
        # Store the units and first value of the time axis.
        if axis.isTime():
            ordering = (axis.units, axis[0])

            logger.info('Axis is time grabbing ordering %r', ordering)

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

        logger.debug('Setting axis %r value to %r', axis.id, axis_data[axis.id])

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


def merge_variables(context, files, data, domain):
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
        files: A list of cdms2.CdmsFile objects to process.
        data: A dask.Array containing the variable.
        domain: A dict mapping axis names to slices.

    Returns:
        Three dicts; the first contains the global attributes, taken from the first file.
        The second is a dict mapping axis names to a dict containing the axis attributes
        and data. The third is a dict mapping varibale names to a dict containing the
        variable data and attributes.
    """
    vars = {}
    axes = {}
    gattrs = None

    logger.info('Merging %r variables', len(files))

    var_name = context.inputs[0].var_name

    for infile in files:
        processed_time = False

        # Grab global attributes from first file.
        if gattrs is None:
            gattrs = infile.attributes

        # Iterate over variables in file
        for v in infile.getVariables():
            present = all(True if x.id in domain else False for x in v.getAxisList() if x.id != 'nbnd')

            logger.debug('Processing variable %r', v.id)

            # If this is not the variable being processed and all axes are not in the domain
            # we assume the variable is not required e.g. we sum over the time axis, then the
            # time_bnds which has the time axis is no longer required.
            if v.id != var_name and not present:
                logger.debug('Skipping %r not all axes present in domain', v.id)

                continue

            for a in v.getAxisList():
                logger.debug('Processing axis %r', a.id)

                if a.isTime() and not processed_time:
                    # Verify that time axis is in domain, this is used when the variable is processed
                    # over the time axis which no longer needs to be included in the output.
                    if a.id in domain:
                        if a.id in axes:
                            axes[a.id]['data'].append(a.clone())
                        else:
                            axes[a.id] = {'data': [a.clone(), ], 'attrs': a.attributes.copy()}
                        processed_time = True
                elif a.id not in axes:
                    asel = domain.get(a.id, slice(None, None, None))
                    i, j, k = slice_to_ijk(a, asel)
                    axes[a.id] = {'data': a.subAxis(i, j, k), 'attrs': a.attributes.copy()}

            if v.id == var_name:
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
                # Only list axes in the domain, special case for nbnd axis
                vars[v.id]['axes'] = [x.id for x in v.getAxisList() if x.id in domain or x.id == 'nbnd']

    logger.info('Variables %r', vars.keys())
    logger.info('Axes %r', axes.keys())

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


def output_with_attributes(context, var_name, gattrs, axes, vars):
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

        logger.info('Built axis %r array', name)

    # Create xarray.DataArray for all variables
    for name, var in list(vars.items()):
        coords = {}

        # Grab the xarray.DataArrays containing the axes in the varibale
        for a in var['axes']:
            coords[a] = xr_axes[a]

        xr_vars[name] = xr.DataArray(var['data'], name=name, dims=coords.keys(), coords=coords, attrs=var['attrs'])

        logger.info('Built variable array %r axes %r', name, xr_vars[name].dims)

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

    old_shape = shape

    # Remove the old value
    shape.pop(index)

    # Replace with the new valur.
    shape.insert(index, diff)

    logger.info('Updated shape %r -> %r', old_shape, shape)

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

    logger.info('Building ingress for %r with %r time steps', url, size)

    # Get index of the time access, may not always be index 0.
    index = var.getAxisIndex('time')

    # Get the step value to generate chunks
    step = chunk_shape[index]

    chunks = [{'time': slice(x, min(x+step, size), 1)} for x in range(0, size, step)]

    logger.debug('Generate chunks %r', chunks)

    # Create a single delayed function for each chunk. Create a dask.Array from the delayed
    # function.
    da_chunks = [da.from_delayed(dask.delayed(retrieve_chunk)(url, var.id, x, cert),
                                 update_shape(list(var.shape), x['time'], 0),
                                 var.dtype)
                 for x in chunks]

    # Combine all dask.Arrays along the time axis
    concat = da.concatenate(da_chunks, axis=0)

    logger.info('Built ingress graph %r', concat)

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
        logger.info('Building regrid graph')

        context.job.update('Building regrid graph')

        # Create a dict mapping axis names to a tuple of the first and last value for each axis present in
        # the selector.
        new_selector = dict((x, (axes[x]['data'][0], axes[x]['data'][-1])) for x, y in list(selector.items()))

        logger.debug('New selector %r', new_selector)

        # Creates a grid thats a subRegion defined by the `new_selector`
        grid, tool, method = context.regrid_context(new_selector)

        context.job.update('Setting target grid to {!r} using {!r} tool and {!r} method', grid.shape, tool, method)

        var_name = context.inputs[0].var_name

        data = vars[var_name]['data']

        logger.debug('Input data %r', data)

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

        logger.info('New data %r', vars[var_name]['data'])

        context.job.update('Finished building regrid graph')

        axes['lat']['data'] = grid.getLatitude()

        vars['lat_bnds']['data'] = grid.getLatitude().getBounds()

        axes['lon']['data'] = grid.getLongitude()

        vars['lon_bnds']['data'] = grid.getLongitude().getBounds()


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

    logger.info('Combining %r maps over axis %r', len(maps), index)

    for x in maps:
        # Grab the axis_slice being combined
        axis_slice = list(x.values())[index]

        if axis_slice is None:
            continue

        # Use the first map as a template
        if template is None:
            template = x.copy()

            logger.debug('Set template to %r', template)

        if start is None:
            # First time being processed, set initial values
            start = axis_slice.start

            stop = axis_slice.stop

            step = axis_slice.step

            logger.debug('Setting initial slice to %r %r %r', start, stop, step)
        else:
            # Extend the stop by the size of the axis
            stop += axis_slice.stop - axis_slice.start

            logger.debug('Extending slice to %r', stop)

    # Find the key of the axis being combined
    key = list(template.keys())[index]

    # Update the axis with the new slice
    template[key] = slice(start, stop, step)

    logger.info('Combined maps %r', template)

    return template


def open_input(input):
    """ Handles opening an input.

    Args:
        input: A cwt.Variable that will be opened.

    Returns:
        A cdms2.FileVariable described by the `input` argument.
    """
    try:
        f = cdms2.open(input.uri)
    except cdms2.CDMSError as e:
        logger.exception('Error opening %r: %s', input.uri, e)

        raise AccessError(input.uri, e)

    logger.info('Opened %r', f.id)

    var = f[input.var_name]

    if var is None:
        raise WPSError('Error accessing variable, {!r} is not present in {!r}', input.var_name, input.uri)

    logger.info('Got input variable %r', var.id)

    return f, var


def build_subset(context, var, cert):
    """ Build a dask subset graph.

    Args:
        context: A wps.context.OperationContext.
        var: A cdms2.FileVariable describe by `input`.
        cert: A str SSL certificate/key used to access ESGF data.
    """
    # Convert cwt.Domain to dict mapping axis names to base types.
    domain = domain_to_dict(context.domain)

    # Map domain to variable axes.
    map, _ = map_domain(var, domain)

    context.job.update('Mapped domain to input file')

    # Determine a base chunk value.
    chunks = (100,) + var.shape[1:]

    # Construct the input dask.Array. If certificates are not required
    # the Array is create from the cdms2.FileVariable otherwise we created
    # delayed functions so the certificate can be written local to the dask
    # worker enabling access to protected ESGF data.
    if cert is None:
        data = da.from_array(var, chunks=chunks)
    else:
        data = build_ingress(var, cert, chunks)

    logger.info('Input data %r', data)

    # Create selector from the mapped slices.
    selector = tuple(map.values())

    logger.info('Subset selector %r', selector)

    # Subset the dask.Array.
    subset_data = data[selector]

    logger.info('Subset data %r', subset_data)

    context.job.update('Subsetting inputs {!r}', subset_data.shape)

    return subset_data, map


def build_aggregate(context, vars, cert):
    """ Build a dask aggregate graph.

    Args:
        context: A wps.context.OperationContext.
        vars: A list of cdms2.FileVariable describe by `input`.
        cert: A str SSL certificate/key used to access ESGF data.
    """
    # Convert domain to dict.
    domain = domain_to_dict(context.domain)

    # Map the domain over each input variable
    maps = [(x, ) + map_domain(x, domain) for x in vars]

    context.job.update('Mapped domain to input files')

    # True if all units are unique.
    order_by_units = len(set(x[-1][0] for x in maps)) == len(context.inputs)
    # True if all first values are unique.
    order_by_first = len(set(x[-1][1] for x in maps)) == len(context.inputs)

    if order_by_units:
        # Order the input maps by units of the time axis.
        ordered_inputs = sorted(maps, key=lambda x: x[-1][0])

        logger.info('Ordering inputs by units %r', [x[-1][0] for x in ordered_inputs])

        context.job.update('Ordered inputs by units')
    elif order_by_first:
        # Order the input maps by the first value in the time axis.
        ordered_inputs = sorted(maps, key=lambda x: x[-1][1])

        logger.info('Ordering inputs by first values %r', [x[-1][1] for x in ordered_inputs])

        context.job.update('Ordered inputs by first values')
    else:
        raise WPSError('Unable to determine the order of inputs')

    chunks = None

    data = []

    for var, _, _ in ordered_inputs:
        if chunks is None:
            chunks = (100,) + var.shape[1:]

        if cert is None:
            data.append(da.from_array(var, chunks=chunks))
        else:
            data.append(build_ingress(var, cert, chunks))

        logger.info('Creating input array %r', data[-1])

    context.job.update('Built {!r} input arrays', len(data))

    # Concatenated inputs over time axis.
    concat = da.concatenate(data, axis=0)

    context.job.update('Concatenating inputs {!r}', concat.shape)

    logger.info('Concatenated input arrays %r', concat)

    # Combind the maps over the time axis
    combined = combine_maps([x[1] for x in maps], 0)

    logger.info('Combined maps %r', combined)

    # Create selector from combined slices
    selector = tuple(combined.values())

    # Take the subset of the concatenated inputs
    subset_data = concat[selector]

    logger.info('Subset data %r', subset_data)

    context.job.update('Subsetting inputs {!r}', subset_data.shape)

    return subset_data, combined


def build_process(context, data, process, map):
    axes = context.operation.get_parameter('axes', True)

    context.job.update('Creating process graph over axes {!r}', axes.values)

    axis_keys = list(map.keys())

    axes_index = [axis_keys.index(x) for x in axes.values]

    data = process(data, axis=tuple(axes_index))

    for x in axes_index:
        map.pop(axis_keys[x])

    logger.info('Process %r created new map %r', data, map)

    context.job.update('Done creating process graph {!r}', data.shape)

    return data


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

    cert = context.user.auth.cert if check_access(context, context.inputs[0]) else None

    file, var = open_input(context.inputs[0])

    # Build the subset dask.Array.
    data, map = build_subset(context, var, cert)

    # Apply process function is present.
    if process is not None:
        data = build_process(context, data, process, map)

    # Collect variable/axis data and attributes
    gattrs, axes, vars = merge_variables(context, [file, ], data, map)

    # Build regrid graph if requested.
    build_regrid(context, axes, vars, map)

    # Build the output with original attributes
    dataset = output_with_attributes(context, context.inputs[0].var_name, gattrs, axes, vars)

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

    cert = context.user.auth.cert if check_access(context, context.inputs[0]) else None

    vars = [open_input(x) for x in context.inputs]

    data, map = build_aggregate(context, [x[1] for x in vars], cert)

    # Merge the variables over the time axis
    gattrs, axes, vars = merge_variables(context, [x[0] for x in vars], data, map)

    # Build regrid graph if requested.
    build_regrid(context, axes, vars, map)

    # Build output with original attributes
    dataset = output_with_attributes(context, context.inputs[0].var_name, gattrs, axes, vars)

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
