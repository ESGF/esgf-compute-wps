#! /usr/bin/env python

import copy
import types
import re
import os
from contextlib import contextmanager
from functools import partial
from xml.sax.saxutils import escape

import cdms2
import cwt
import dask
import dask.array as da
import metpy  # noqa F401
import numpy as np
import requests
import tempfile
import xarray as xr
import xarray.ufuncs as xu
from celery.utils.log import get_task_logger
from dask.distributed import Client
from distributed.diagnostics.progressbar import ProgressBar
from distributed.utils import LoopRunner
from distributed.client import futures_of
from jinja2 import Environment, BaseLoader
from tornado.ioloop import IOLoop

from compute_tasks import base
from compute_tasks import context as ctx
from compute_tasks.context import state_mixin
from compute_tasks import managers
from compute_tasks import WPSError
from compute_tasks.dask_serialize import regrid_chunk

logger = get_task_logger('compute_tasks.cdat')


class DaskJobTracker(ProgressBar):
    def __init__(self, context, futures, scheduler=None, interval='100ms', complete=True):
        """ Init method.

        Class init method.

        Args:
            context (context.OperationContext): The current context.
            futures (list): A list of futures to track the progress of.
            scheduler (dask.distributed.Scheduler, optional): The dask scheduler being used.
            interval (str, optional): The interval used between posting updates of the tracked futures.
            complete (bool, optional): Whether to callback after all futures are complete.
        """
        futures = futures_of(futures)

        context.message('Tracking {!r} futures', len(futures))

        super(DaskJobTracker, self).__init__(futures, scheduler, interval, complete)

        self.context = context
        self.last = None

        self.loop = IOLoop()

        loop_runner = LoopRunner(self.loop)
        loop_runner.run_sync(self.listen)

    def _draw_bar(self, **kwargs):
        """ Update callback.

        After each interval this method is called allowing for a update of the progress.

        Args:
            remaining (int): The number of remaining futures to be processed.
            all (int): The total number of futures to be processed.
        """
        logger.debug('_draw_bar %r', kwargs)

        remaining = kwargs.get('remaining', 0)

        all = kwargs.get('all', None)

        frac = (1 - remaining / all) if all else 1.0

        percent = int(100 * frac)

        logger.debug('Percent processed %r', percent)

        if self.last is None or self.last != percent:
            self.context.message('Processing', percent=percent)

            self.last = percent

    def _draw_stop(self, **kwargs):
        pass


def clean_variable_encoding(dataset):
    """ Cleans the encoding for a variable.

    Sometimes a variable's encoding will have missing_value and _FillValue, xarray does not like
    this, thus one must be removed. ``missing_value`` was arbitrarily selected to be removed.

    Args:
        dataset (xarray.DataSet): The dataset to check each variables encoding settings.
    """
    for name, variable in dataset.variables.items():
        if 'missing_value' in variable.encoding and '_FillValue' in variable.encoding:
            del variable.encoding['missing_value']

    return dataset


def gather_workflow_outputs(context, interm, operations):
    """ Gather the ouputs for a set of processes (operations).

    For each process we find it's output. Next the output path for the file is created. Next
    the Dask delayed function to create a netCDF file is created and added to a list that is
    returned.

    Args:
        context (context.OperationContext): The current context.
        interm (dict): A dict mapping process names to Dask delayed functions.
        operations (list): A list of ``cwt.Process`` objects whose outputs are being gathered.

    Returns:
        list: Of Dask delayed functions outputting netCDF files.

    Raises:
        WPSError: If output is not found or there exists and issue creating the output netCDF file.
    """
    delayed = []

    for output in operations:
        try:
            interm_ds = interm.pop(output.name)
        except KeyError as e:
            raise WPSError('Failed to find intermediate {!s}', e)

        context.track_out_bytes(interm_ds.nbytes)

        output_name = '{!s}-{!s}'.format(output.name, output.identifier)

        local_path = context.build_output_variable(context.variable, name=output_name)

        context.message('Building output for {!r} - {!r}', output.name, output.identifier)

        logger.debug('Writing local output to %r', local_path)

        interm_ds = clean_variable_encoding(interm_ds)

        try:
            # Create an output file and store the future
            delayed.append(interm_ds.to_netcdf(local_path, compute=False, format='NETCDF3_64BIT', engine='netcdf4'))
        except ValueError:
            shapes = dict((x, y.shape[0] if len(y.shape) > 0 else None) for x, y in interm_ds.coords.items())

            bad_coords = ', '.join([x for x, y in shapes.items() if y == 1])

            raise WPSError('Domain resulted in empty coordinates {!s}', bad_coords)

    return delayed


def check_access(url, cert=None):
    """ Checks if url is accessible.

    Checks if a remote file is accessible by the service. It's expecting an OpenDAP url, to check for this an
    HTTP GET request is executed against the `.dds` path. Allows the passing of a SSL client certificate in
    cases such as ESGF's protected data.

    Args:
        url (str): The URL to check.
        cert (str): A path to an SSL client certificate.

    Returns:
        bool: True if the HTTP status code is 200, False if its 401 or 403.

    Raises:
        WPSError: If the status code is anything other than 200, 401 or 403.
    """
    dds_url = '{!s}.dds'.format(url)

    try:
        # TODO bootstrap trust roots and set verify True
        response = requests.get(dds_url, timeout=(5, 15), cert=cert, verify=False)
    except Exception as e:
        raise WPSError('Failed to access input {!s}, {!s}'.format(url, e))

    logger.info('Server response %r', response.status_code)

    if response.status_code == 200:
        return True

    if response.status_code in (401, 403):
        return False

    raise WPSError('Failed to access input {!r}, reason {!s} ({!s})', url, response.reason, response.status_code)


@contextmanager
def chdir_temp():
    """ Changes current working directory.

    Args:
        dirname (str): A directory name to change into.
    """
    old_cwd = os.getcwd()

    with tempfile.TemporaryDirectory() as tempdir:
        os.chdir(tempdir)

        logger.info('Changed directory %s -> %s', old_cwd, tempdir)

        try:
            yield tempdir
        finally:
            os.chdir(old_cwd)

            logger.info('Changed directory %s -> %s', tempdir, old_cwd)


def write_dodsrc(cert_file):
    """ Writes a dodsrc file in the current working directory.

    Args:
        cert_file (str): A string path to the certificate file.
    """
    cwd = os.getcwd()

    dodsrc_file = os.path.join(cwd, '.dodsrc')

    with open(dodsrc_file, 'w') as outfile:
        outfile.write('HTTP.COOKIEJAR=.cookies\n')
        outfile.write('HTTP.SSL.CERTIFICATE={!s}\n'.format(cert_file))
        outfile.write('HTTP.SSL.KEY={!s}\n'.format(cert_file))
        outfile.write('HTTP.SSL.CAPATH={!s}\n'.format(cert_file))
        outfile.write('HTTP.SSL.VALIDATE=false\n')

    logger.info('Wrote dodsrc to %s', dodsrc_file)


def update_shape(shape, index, size):
    """ Updates a value in a list.

    Args:
        shape (list): A list that will have a value updated.
        index (int): Index of the value to be updated.
        size (int): An int value to replace at `index`.

    Returns:
        A new list whose value a `index` has been update with `size`.
    """
    shape.pop(index)

    shape.insert(index, size)

    return shape


def get_protected_data(url, var_name, cert, **chunk):
    """ Retrieves a chunk of data requiring a client certificate.

    Create a temporary directory where the client certificate and a
    .dodsrc file are written for the netCDF library to use when
    accessing the remote file. A chunk of data whose domain is
    `chunk` is retrieved and returned.

    Args:
        url (str): The url to retrieve the chunk of data from.
        var_name (str): The variable name.
        cert (str): The client certificate.
        chunk (dict): A dict containing dimension names and slices.

    Returns:
        An xarray.DataArray containing the chunk of data.
    """
    with chdir_temp() as tempdir:
        cert_file = os.path.join(tempdir, 'cert.pem')

        with open(cert_file, 'w') as outfile:
            outfile.write(cert)

        logger.info('Wrote certificate to %s', cert_file)

        write_dodsrc(cert_file)

        ds = xr.open_dataset(url)

        data = ds.isel(chunk)[var_name]

    logger.info('Returning chunk with %s bytes', data.nbytes)

    return data


def build_dask_array(url, var_name, dataarray, chunks, cert):
    """ Builds a Dask array from an xarray data array.

    This will build a dask array from delayed functions which
    will use a client certificate to access a protected opendap
    endpoint.

    Args:
        url (str): Remote file url.
        var_name (str): Variable name to create array from.
        dataarray (xarray.DataArray): Source data array.
        chunks (dict): Maps dimension name to chunk size.
        cert (str): A client certificate to access remote opendap data.

    Returns:
        A dask.DataArray that represents the input `dataarray`.
    """
    logger.info('Building dask array for variable %s', var_name)

    # TODO could be built out to handle chunking over multiple dimensions.
    chunk_name = list(chunks.keys())[0]

    chunk_step = list(chunks.values())[0]

    logger.info('Chunk name %s step %s', chunk_name, chunk_step)

    index = dataarray.get_axis_num(chunk_name)

    size = dataarray.shape[index]

    shape = list(dataarray.shape)

    logger.info('Chunk index %s size %s, data array shape %s', index, size, shape)

    chunk_slices = [slice(x, min(x+chunk_step, size)) for x in range(0, size, chunk_step)]

    chunk_shapes = [update_shape(shape.copy(), index, x.stop-x.start) for x in chunk_slices]

    logger.info('Split variable into %s chunks', len(chunk_slices))

    delayed = [dask.delayed(get_protected_data)(url, var_name, cert, time=x) for x in chunk_slices]

    dask_da = [da.from_delayed(y, shape=x, dtype=dataarray.dtype) for x, y in zip(chunk_shapes, delayed)]

    concat = da.concatenate(dask_da, axis=0)

    logger.info('Created dask array shape %s', concat.shape)

    return concat


def build_dataarray(url, var_name, da, chunks, cert):
    """ Builds an xarray DataArray from a Dask Array.

    Builds an equivelent DataArray of `da` from a Dask Array
    built from delayed functions allowing accessing to protected
    opendap data.

    Args:
        url (str): Remote file url.
        var_name (str): Variable name to create array from.
        da (xarray.DataArray): Source data array.
        chunks (dict): Maps dimension name to chunk size.
        cert (str): A client certificate to access remote opendap data.

    Returns:
        An xarray DataArray which is the equivelent of `da`.
    """
    logger.info('Building dataarray')

    dask_array = build_dask_array(url, var_name, da, chunks, cert)

    data_array = xr.DataArray(dask_array, dims=da.dims, coords=da.coords, name=da.name, attrs=da.attrs)

    return data_array


def build_dataset(url, var_name, ds, chunks, cert):
    """ Builds an xarray DataSet.

    Builds an equivelent DataSet of `ds`.

    Args:
        url (str): Remote file url.
        var_name (str): Variable name to create array from.
        da (xarray.DataArray): Source data array.
        chunks (dict): Maps dimension name to chunk size.
        cert (str): A client certificate to access remote opendap data.

    Returns:
        An xarray DataSet equivelent to `ds`.
    """
    logger.info('Building dataset')

    data_array = build_dataarray(url, var_name, ds[var_name], chunks, cert)

    data_vars = dict(ds.data_vars)

    data_vars[var_name] = data_array

    data_set = xr.Dataset(data_vars, attrs=ds.attrs)

    return data_set


def open_protected_dataset(context, url, var_name, chunks, decode_times=False):
    """ Opens a protected dataset.

    This will rebuild a DataSet using Dask delayed functions to
    access protected opendap data.

    Args:
        context (compute_tasks.context.OperationContext): Current context.
        url (str): Remote file url.
        var_name (str): Variable name to create array from.
        chunks (dict): Maps dimension name to chunk size.
        cert_data (str): A client certificate to access remote opendap data.
        cert_file (str): A path to a local client certificate use to access the remote DataSet.

    Returns:
        An xarray DataSet.
    """
    logger.info('Opening protected dataset %s', url)

    cert_data = context.user_cert()

    logger.info('Retrieving user certificate')

    with chdir_temp() as tempdir:
        cert_file = os.path.join(tempdir, 'cert.pem')

        with open(cert_file, 'w') as outfile:
            outfile.write(cert_data)

        logger.info('Wrote certificate to %s', cert_file)

        write_dodsrc(cert_file)

        if not check_access(url, cert_file):
            raise WPSError('Failed to access input {!r}', url)

        ds = xr.open_dataset(url, engine='netcdf4', decode_times=decode_times)

        ds = build_dataset(url, var_name, ds, chunks, cert_data)

    return ds


def open_dataset(context, url, var_name, chunks, decode_times=True):
    """ Opens a remote opendap dataset.

    This will directly open a remote opendap dataset or build an
    xarray DataSet capable of accessing a protected opendap url
    using a client certificate.

    Args:
        context (compute_tasks.context.OperationContext): Current context.
        url (str): Remote file url.
        var_name (str): Variable name to create array from.
        chunks (dict): Maps dimension name to chunk size.

    Returns:
        An xarray DataSet.
    """
    logger.info('Opening dataset %r', url)

    if not check_access(url):
        ds = open_protected_dataset(context, url, var_name, chunks, decode_times)
    else:
        ds = xr.open_dataset(url, engine='netcdf4', chunks=chunks, decode_times=decode_times)

    return ds


def gather_inputs(context, process):
    """ Gathers the inputs for a process.

    The inputs for a process which are assumed to be OpenDAP urls to CF compliant data files are
    converted to ``xarray.DataSet`` objects which are lazy loaded. If an input is no accessible
    due to requiring authorization e.g. ESGF CMIP5/CMIP3 then we need to localize the file and
    cache it. The ability to lazy load the remote data is not supported at the moment,
    ``xarray.backends.PydapDataStore`` does not work with Dask clusters, due to a recusion error
    in the Pydap data model.

    Args:
        context (context.OperationContext): Current context.
        process (cwt.Process): The process whose inputs are being gathered.

    Returns:
        list: A list of ``xarray.DataSet``s.
    """
    # TODO make chunks smarter
    chunks = {'time': 100}

    decode_times = True

    if process.domain is not None:
        # TODO improve find the time axis if it exists, it may not be named time.
        time = process.domain.get_dimension('time')

        if time is not None and time.crs == cwt.VALUES:
            decode_times = False

    logger.info('Decode times %r', decode_times)

    datasets = [open_dataset(context, x.uri, x.var_name, chunks, decode_times) for x in process.inputs]

    if process.identifier == 'CDAT.aggregate':
        logger.info('Combining %s datasets', len(datasets))

        datasets = [xr.combine_by_coords(datasets)]

    return datasets


def build_workflow(context):
    """ Builds a workflow.

    The processes are processed intopological order. When processed the inputs are gathered,
    current these are limited to either are variable inputs or all intermediate products, eventually
    a mix of inputs will be accepted. The appropriate process is applied to the inputs and the
    output is stored for future use.

    Args:
        context (context.OperationContext): Current context.
    """
    # Hold the intermediate inputs
    interm = {}

    for next in context.topo_sort():
        context.message('Processing operation {!r} - {!r}', next.name, next.identifier)

        if all(isinstance(x, cwt.Variable) for x in next.inputs):
            inputs = gather_inputs(context, next)
        else:
            try:
                inputs = [interm[x.name] for x in next.inputs]
            except KeyError as e:
                raise WPSError('Missing intermediate data {!s}', e)

            # Create copy of single input, multiple inputs automatically create a new output
            if len(inputs) == 1:
                inputs = [inputs[0].copy(), ]

        process_func = PROCESS_FUNC_MAP[next.identifier]

        if process_func is None:
            interm[next.name] = subset_input(context, next, inputs[0].copy())
        else:
            inputs = [subset_input(context, next, x) for x in inputs]

            interm[next.name] = process_func(context, next, *inputs)

        context.message('Storing intermediate {!r}', next.name)

    return interm


def execute_delayed(context, delayed, client=None):
    """ Executes a list of Dask delayed functions.

    The list of Dask delayed functions are executed locally unless a client is defined. When a
    client is passed the delays are execute and futures are returned, these are then tracked
    using ``DaskJobTracker`` to update the user on the progress of execution. Thise is all wrapped
    by a `context.ProcessTimer` which times the whole event then updates the metrics.

    Args:
        context (context.OperationContext): Current context.
        delayed (list): List of Dask delayed functions.
        client (dask.distributed.Client): Client to execute the Dask delays.
    """
    with ctx.ProcessTimer(context):
        if client is None:
            dask.compute(delayed)
        else:
            fut = client.compute(delayed)

            DaskJobTracker(context, fut)


def workflow(context):
    """ Executes a workflow.

    A Celery task for executing a workflow of processes. The workflow is built then the
    intermediate and output Dask delayed functions are gathered. These are then executed
    Dask.

    Args:
        context (OperationContext): The context containing all information needed to execute process.

    Returns:
        The input context for the next Celery task.
    """
    interm = build_workflow(context)

    context.message('Preparing to execute workflow')

    if 'DASK_SCHEDULER' in context.extra:
        client = state_mixin.retry(8, 1)(Client)(context.extra['DASK_SCHEDULER'])
    else:
        client = None

    try:
        delayed = []

        delayed.extend(gather_workflow_outputs(context, interm, context.output_ops()))

        # TODO possibly re-enable after solving slow large outputs. Also should move
        # this to individual processes.
        # if 'store_intermediates' in context.gparameters:
        #     delayed.extend(gather_workflow_outputs(context, interm, context.interm_ops()))

        context.message('Gathered {!s} outputs', len(delayed))

        execute_delayed(context, delayed, client)
    except WPSError:
        raise
    except Exception as e:
        raise WPSError('Error executing process: {!r}', e)

    return context


def subset_input(context, operation, input):
    """ Subsets a Dataset.

    Subset a ``xarray.Dataset`` using the user-defined ``cwt.Domain``. For each dimension
    the selector is built and applied using the appropriate method.

    Args:
        context (context.OperationContext): The current operation context.
        operation (cwt.Process): The process definition the input is associated with.
        input (xarray.DataSet): The input DataSet.
    """
    context.track_src_bytes(input.nbytes)

    if operation.domain is not None:
        for dim in operation.domain.dimensions.values():
            selector = {dim.name: slice(dim.start, dim.end, dim.step)}

            if dim.crs == cwt.INDICES:
                input = input.isel(**selector)
            elif dim.crs == cwt.VALUES:
                input = input.loc[selector]
            else:
                input = input.sel(**selector)

    # Check if domain was outside the inputs domain.
    for x, y in input.dims.items():
        if y == 0:
            raise WPSError('Domain for process {!r} resulted in a zero length dimension {!r}', operation.identifier, x)

    context.track_in_bytes(input.nbytes)

    return input


def rename_variable(var_name, input, operation, output):
    if isinstance(input, xr.core.groupby.DatasetGroupBy):
        name = operation.identifier.split('.')[-1]

        output = output.rename({var_name: name})

    return output


def process_elementwise(context, operation, *input, **kwargs):
    func = kwargs['func']

    v = context.variable

    output = input[0].copy()

    input_var = output[v]

    output[v] = func(input_var)

    return rename_variable(v, input[0], operation, output)


def process_reduce(context, operation, *input, **kwargs):
    func = kwargs['func']

    v = context.variable

    axes = operation.get_parameter('axes') or None

    output = input[0].copy()

    output[v] = func(input[0][v], axes.values)

    return rename_variable(v, input[0], operation, output)


def process_dataset(context, operation, *input, **kwargs):
    func = kwargs['func']

    v = context.variable

    output = input[0].copy()

    if len(input) == 1:
        output[v] = func(input[0][v])
    else:
        raise WPSError('Process {!s} only requires 1 input', operation.identifier)

    return rename_variable(v, input[0], operation, output)


def process_dataset_or_const(context, operation, *input, **kwargs):
    func = kwargs['func']

    v = context.variable

    const = operation.get_parameter('const')

    output = input[0].copy()

    if const is None:
        if len(input) == 2:
            output[v] = func(input[0][v], input[1][v])
        else:
            raise WPSError('Process {!s} requires 2 inputs or "const" parameter.', operation.identifier)
    else:
        try:
            const = float(const.values[0])
        except ValueError:
            raise WPSError('Invalid value {!r} with type {!s}, expecting a float value.', constant, type(constant))

        output[v] = func(input[0][v], const)

    return rename_variable(v, input[0], operation, output)


def process_merge(context, operation, *input, **kwargs):
    input = list(input)

    root = input.pop()

    context.message('Merging {!s} variables', len(input))

    for x in input:
        root = root.merge(x)

    context.message('Done merging variables')

    return root


def process_where(context, operation, *input, **kwargs):
    cond = operation.get_parameter('cond')

    if cond is None:
        raise WPSError('Missing parameter "cond"')

    match = re.match('(?P<left>\w+)(?P<comp>[<>=!]{1,2})(?P<right>-?\d+\.?\d?)', cond.values[0])

    if match is None:
        raise WPSError('Condition is not valid, check abstract')

    comp = match['comp']

    left = match['left']

    if left not in input[0]:
        raise WPSError('Did not find {!s} in input', left)

    right = float(match['right'])

    context.message('Applying where with condition "{!s}{!s}{!s}"', left, comp, right)

    if comp == ">":
        output = input[0].where(input[0][left]>right)
    elif comp == ">=":
        output = input[0].where(input[0][left]>=right)
    elif comp == "<":
        output = input[0].where(input[0][left]<right)
    elif comp == "<=":
        output = input[0].where(input[0][left]<=right)
    elif comp == "==":
        output = input[0].where(input[0][left]==right)
    elif comp == "!=":
        output = input[0].where(input[0][left]!=right)
    else:
        raise WPSError('Comparison with {!s} is not supported', comp)

    fillna = operation.get_parameter('fillna')

    if fillna is not None:
        try:
            fillna = float(fillna.values[0])
        except ValueError:
            raise WPSError('Could not convert the "fillna" value to float.')

        context.message('Filling nan with {!s}', fillna)

        output = output.fillna(fillna)

    return output


def process_groupby_bins(context, operation, *input, **kwargs):
    v = operation.get_parameter('variable', True)

    variable = v.values[0]

    if variable not in input[0]:
        raise WPSError('Did not find variable {!s} in input.', variable)

    b = operation.get_parameter('bins', True)

    try:
        bins = [float(x) for x in b.values]
    except ValueError:
        raise WPSError('Failed to convert a bin value.')

    context.message('Grouping {!s} into bins {!s}', variable, bins)

    groups = input[0].groupby_bins(variable, bins)

    return groups

# Two parent types
# 1. Operating on a variable
# 2. Operating on a Dataset e.g. resample, groupby

PROCESS_FUNC_MAP = {
    'CDAT.abs': partial(process_elementwise, func=lambda x: np.abs(x)),
    'CDAT.add': partial(process_dataset_or_const, func=lambda x, y: x + y),
    'CDAT.aggregate': None,
    'CDAT.divide': partial(process_dataset_or_const, func=lambda x, y: x / y),
    'CDAT.exp': partial(process_elementwise, func=lambda x: np.exp(x)),
    'CDAT.log': partial(process_elementwise, func=lambda x: np.log(x)),
    'CDAT.max': partial(process_reduce, func=lambda x, y: getattr(x, 'max')(dim=y, keep_attrs=True)),
    'CDAT.mean': partial(process_reduce, func=lambda x, y: getattr(x, 'mean')(dim=y, keep_attrs=True)),
    'CDAT.min': partial(process_reduce, func=lambda x, y: getattr(x, 'min')(dim=y, keep_attrs=True)),
    'CDAT.multiply': partial(process_dataset_or_const, func=lambda x, y: x * y),
    'CDAT.power': partial(process_dataset_or_const, func=lambda x, y: x ** y),
    'CDAT.subset': None,
    'CDAT.subtract': partial(process_dataset_or_const, func=lambda x, y: x - y),
    'CDAT.sum': partial(process_reduce, func=lambda x, y: getattr(x, 'sum')(dim=y, keep_attrs=True)),
    'CDAT.merge': process_merge,
    'CDAT.where': process_where,
    'CDAT.groupby_bins': process_groupby_bins,
    'CDAT.count': partial(process_dataset, func=lambda x: getattr(x, 'count')()),
    'CDAT.squeeze': partial(process_dataset, func=lambda x: getattr(x, 'squeeze')(drop=True)),
    'CDAT.std': partial(process_reduce, func=lambda x, y: getattr(x, 'std')(dim=y, keep_attrs=True)),
    'CDAT.var': partial(process_reduce, func=lambda x, y: getattr(x, 'var')(dim=y, keep_attrs=True)),
    'CDAT.workflow': None,
}


BASE_ABSTRACT = """
{{- description }}
{% if min > 0 and (max == infinity) %}
Accepts a minimum of {{ min }} inputs.
{% elif min == max %}
Accepts exactly {{ min }} input.
{% elif max > min %}
Accepts {{ min }} to {{ max }} inputs.
{% endif %}
{%- if params|length > 0 %}
Parameters:
{%- for group in params|groupby('required')|reverse %}
    {%- for item in group.list %}
    {{ item['name'] }} ({{ item['type'] }}, {{ group.grouper }}): {{ item['desc'] }}
    {%- endfor %}
{%- endfor %}
{% endif %}
"""


template = Environment(loader=BaseLoader).from_string(BASE_ABSTRACT)


def render_abstract(identifier, description, **params):
    """ Renders an abstract for a process.

    This function will use a jinja2 template and render out an abstract for a process. The keyword arguments
    for the ``func`` function are used to enable details in the abstract.

    Args:
        description (str): The process description.
        min_inputs (int): Minimum number of inputs.
        max_inputs (int): Maximum number of inputs.

    Returns:
        str: The abstract as a string.
    """
    kwargs = {
        'description': description,
        'min': params.pop('min', 1),
        'max': params.pop('max', 1),
        'params': [],
        'infinity': float('inf'),
    }

    for x, y in params.items():
        new = copy.deepcopy(y)

        new['name'] = x

        new['type'] = y['type'].__name__

        new['desc'] = PARAMS_DESC[x]

        new['required'] = 'Required' if y['required'] else 'Optional'

        kwargs['params'].append(new)

    VALIDATION[identifier] = validation(min=kwargs['min'], max=kwargs['max'], **params)

    ABSTRACT[identifier] = escape(template.render(**kwargs))


PARAMS_DESC = {
    'axes': 'A list of axes to reduce dimensionality over. Separate multiple values with "|" e.g. time|lat.',
    'const': 'A float value that will be applied element-wise.',
    'cond': 'A condition that when true will preserve the value, otherwise the value will be set to "nan".',
    'fillna': 'A float value to replace "nan" values."',
    'variable': 'The variable to process.',
    'bins': 'A list of bins. Separate values with "|" e.g. 0|10|20, this would create 2 bins (0-10), (10, 20).',
}


def validation(**params):
    return {
        'min': params.pop('min', 1),
        'max': params.pop('max', 1),
        'params': params,
    }


def parameter(type, required=False, **params):
    return {
        'required': required,
        'type': type,
        'min': params.pop('min', 1),
        'max': params.pop('max', 1),
    }

WHERE_ABS = """Filters elements based on a condition.

Supported comparisons: >, >=, <, <=, ==, !=

Left hand side should be a variable or axis name and the right can be an int or float.

Examples:
    lon>180
    pr>0.000023408767
"""

WORKFLOW_ABS = """This process is used to store global values in workflows. Domain
and parameters defined here will become the default values on child operations.
"""

VALIDATION = {}
ABSTRACT = {}

render_abstract('CDAT.abs', 'Computes element-wise absolute value.')
render_abstract('CDAT.add', 'Adds two variables or a constant element-wise.', const=parameter(float), max=2)
render_abstract('CDAT.aggregate', 'Aggregates a variable spanning two or more files.', min=2, max=float('inf'))
render_abstract('CDAT.divide', 'Divides a variable by another or a constant element-wise.', const=parameter(float), max=2)
render_abstract('CDAT.exp', 'Computes element-wise exponential value.')
render_abstract('CDAT.log', 'Computes element-wise log value.')
render_abstract('CDAT.max', 'Computes the maximum value over one or more axes.', axes=parameter(str))
render_abstract('CDAT.mean', 'Computes the mean over one or more axes.', axes=parameter(str))
render_abstract('CDAT.min', 'Computes the minimum value over one or more axes.', axes=parameter(str))
render_abstract('CDAT.multiply', 'Multiplies a variable by another or a constant element-wise', const=parameter(float), max=2)
render_abstract('CDAT.power', 'Takes a variable to the power of another variable or a constant element-wise.', const=parameter(float))
render_abstract('CDAT.subset', 'Computes the subset of a variable defined by a domain.')
render_abstract('CDAT.subtract', 'Subtracts a variable from another or a constant element-wise.', const=parameter(float), max=2)
render_abstract('CDAT.sum', 'Computes the sum over one or more axes.', axes=parameter(str))
render_abstract('CDAT.merge', 'Merges variable from second input into first.', min=2, max=float('inf'))
render_abstract('CDAT.where', WHERE_ABS, cond=parameter(str, True), fillna=parameter(float))
render_abstract('CDAT.groupby_bins', 'Groups values of a variable into bins.', variable=parameter(str, True), bins=parameter(float, True))
render_abstract('CDAT.count', 'Computes count on each variable.')
render_abstract('CDAT.squeeze', 'Squeezes data, will drop coordinates.')
render_abstract('CDAT.std', 'Computes the standard deviation over one or more axes.', axes=parameter(str))
render_abstract('CDAT.var', 'Computes the variance over one or more axes.', axes=parameter(str))
render_abstract('CDAT.workflow', WORKFLOW_ABS, max=float('inf'))


def process_wrapper(self, context):
    """ Wrapper function for a process.

    This function acts as the main entrypoint of a Celery task. It represents a single process e.g. Subset, Aggregate,
    etc. The function calls ``workflow_func`` since a single process is just a workflow with a single task.

    Args:
        context (OperationContext): The OperationContext holding all details of the current job.
    """
    return workflow(context)


def copy_function(f, operation):
    """ Creates a unique version of a function.

    Copies function ``f`` giving it a unique name using ``operation``.

    Args:
        f (FunctionType): The function to copy.
        operation (str): The unique identifier for the function.

    Returns:
        FunctionType: A new function.
    """
    name = '{!s}_func'.format(operation)

    return types.FunctionType(f.__code__, f.__globals__, name=name, argdefs=f.__defaults__, closure=f.__closure__)


def discover_processes():
    """ Discovers and binds functions to `cdat` module.

    This function iterates over PROCESS_FUNC_MAP, generating a description, creating a Celery task, registering it with
    the backend and binding it to the "cdat" module.

    Returns:
        list: List of dict, describing each registered process.
    """
    from compute_tasks import base
    from compute_tasks import cdat

    for name, func in PROCESS_FUNC_MAP.items():
        module, operation = name.split('.')

        # Create a unique function for each process
        p = copy_function(process_wrapper, operation)

        # Decorate the new function as a Celery task
        shared = base.cwt_shared_task()(p)

        abstract = ABSTRACT[name]

        # Decorate the Celery task as a registered process
        register = base.register_process(module, operation, abstract=abstract)(shared)

        # Bind the new function to the "cdat" module
        setattr(cdat, p.__name__, register)

        logger.info('Binding process %r', name)

    return base.REGISTRY.values()
