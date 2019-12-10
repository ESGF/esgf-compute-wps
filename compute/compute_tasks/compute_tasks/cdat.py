#! /usr/bin/env python

import types
import re
import os
from functools import partial

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
        response = requests.get(dds_url, timeout=(10, 4), cert=cert, verify=False)
    except Exception as e:
        raise WPSError('Failed to access input {!s}, {!s}'.format(url, e))

    logger.info('Server response %r', response.status_code)

    if response.status_code == 200:
        return True

    if response.status_code in (401, 403):
        return False

    raise WPSError('Failed to access input {!r}, reason {!s} ({!s})', url, response.reason, response.status_code)


def get_user_cert(context):
    """ Stores the user certificate in a temporary file.

    Args:
        context (context.OperationContext): The current context.

    Returns:
        A tempfile.NamedTemporaryFile object.
    """
    tf = tempfile.NamedTemporaryFile()

    cert = context.user_cert()

    tf.write(cert.encode())

    return tf


def localize_protected(context, urls, cert_path):
    """ Localize protected data.

    Attempt to localize protected data. A unique path is generated and checked if it already exists.
    If the cached file does not exist then we localize it. This supports retry attempts for small
    outages.

    Args:
        context (context.OperationContext): The current context.a
        urls (list): A list of urls to localize.
        cert_path (str): A str path to an SSL client certificate used to access the remote data.

    Returns:
        list: Of paths to the localized content.
    """
    new_urls = []

    session = requests.Session()

    session.cert = cert_path

    for url in urls:
        # TODO make a smarter version of cache_local_path.
        cached_path = context.cache_local_path(url)

        # Already exists, skip localizing.
        if not os.path.exists(cached_path):
            store = xr.backends.PydapDataStore.open(url, session=session)

            ds = xr.open_dataset(store)

            # Retry 4 times start with a 1 second delay.
            state_mixin.retry(4, 1)(ds.to_netcdf)(cached_path, format='NETCDF3_64BIT', engine='netcdf4')

        new_urls.append(cached_path)

    return new_urls


def filter_protected(context, urls):
    """ Filters the protected urls.

    Filter the protected urls from the unprotected. Also returns a certificate if
    protected urls are discovered.

    Args:
        context (context.OperationContext): The current context.
        urls (list): List of str urls to check.
    """
    protected = []
    unprotected = []
    cert_tempfile = None

    # Determine which files are accessible
    for x in urls:
        if not check_access(x):
            if cert_tempfile is None:
                cert_tempfile = get_user_cert(context)

            if not check_access(x, cert=cert_tempfile.name):
                raise WPSError('Failed to access input {!r}', x)

            protected.append(x)
        else:
            unprotected.append(x)

    return unprotected, protected, cert_tempfile


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
    chunks = {
        'time': 100,
    }

    urls, protected, cert_tempfile = filter_protected(context, [x.uri for x in process.inputs])

    # Localize any files requiring an SSL client certificate
    # TODO figure out a way to get PydapDataStore to work with dask.distributed,
    # currently fails with recurrsion error.
    if len(protected) > 0:
        protected = localize_protected(context, protected, cert_tempfile.name)

    if process.identifier == 'CDAT.aggregate':
        gathered = [xr.open_mfdataset(urls+protected, chunks=chunks, combine='by_coords')]
    else:
        gathered = [xr.open_dataset(x, chunks=chunks) for x in urls+protected]

    return gathered


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


WORKFLOW_ABSTRACT = """
This operation is used to store global values in workflows. Domain, regridders and parameters defined
here will become the default values on child operations.
"""


@base.register_process('CDAT', 'workflow', abstract=WORKFLOW_ABSTRACT, inputs='*')
@base.cwt_shared_task()
def workflow_func(self, context):
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

        if 'store_intermediates' in context.gparameters:
            delayed.extend(gather_workflow_outputs(context, interm, context.interm_ops()))

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
        try:
            time = list(input[context.variable].metpy.coordinates('time'))[0]
        except (AttributeError, IndexError):
            time = None

        for dim in operation.domain.dimensions.values():
            selector = {dim.name: slice(dim.start, dim.end, dim.step)}

            if dim.crs == cwt.INDICES:
                input = input.isel(**selector)
            elif dim.crs == cwt.VALUES:
                if time is not None and time.name == dim.name:
                    input[time.name] = xr.conventions.encode_cf_variable(input[time.name])

                    input = input.sel(**selector)
                else:
                    input = input.loc[selector]
            else:
                input = input.sel(**selector)

    context.track_in_bytes(input.nbytes)

    return input


def process_elementwise(context, operation, *input, func, **kwargs):
    v = context.variable

    output = input[0].copy()

    output[v] = func(input[0][v])

    return output


def process_reduce(context, operation, *input, func, **kwargs):
    axes = operation.get_parameter('axes', required=True)

    return func(input[0], axes.values)


def process_dataset(context, operation, *input, func, **kwargs):
    v = context.variable

    const = operation.get_parameter('const')

    output = input[0].copy()

    if const is None:
        if len(input) < 2:
            raise WPSError('Process {!r} is expecting 2 inputs', operation.identifier)

        output[v] = func(input[0][v], input[1][v])
    else:
        try:
            const = float(const.values[0])
        except ValueError:
            raise WPSError('Invalid value {!r} with type {!s}, expecting a float value.', constant, type(constant))

        output[v] = func(input[0][v], const)

    return output

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
    'CDAT.add': partial(process_dataset, func=lambda x, y: x + y),
    'CDAT.aggregate': None,
    'CDAT.divide': partial(process_dataset, func=lambda x, y: x / y),
    'CDAT.exp': partial(process_elementwise, func=lambda x: np.exp(x)),
    'CDAT.log': partial(process_elementwise, func=lambda x: np.log(x)),
    'CDAT.max': partial(process_reduce, func=lambda x, y: getattr(x, 'max')(dim=y, keep_attrs=True)),
    'CDAT.mean': partial(process_reduce, func=lambda x, y: getattr(x, 'mean')(dim=y, keep_attrs=True)),
    'CDAT.min': partial(process_reduce, func=lambda x, y: getattr(x, 'min')(dim=y, keep_attrs=True)),
    'CDAT.multiply': partial(process_dataset, func=lambda x, y: x * y),
    'CDAT.power': partial(process_dataset, func=lambda x, y: x ** y),
    'CDAT.subset': None,
    'CDAT.subtract': partial(process_dataset, func=lambda x, y: x - y),
    'CDAT.sum': partial(process_reduce, func=lambda x, y: getattr(x, 'sum')(dim=y, keep_attrs=True)),
    'CDAT.merge': process_merge,
    'CDAT.where': process_where,
    'CDAT.groupby_bins': process_groupby_bins,
}


BASE_ABSTRACT = """
{{- description }}
{% if min_inputs > 0 and max_inputs is none %}
A minimum of {{ min_inputs }} is required.
{%- elif min_inputs == 1 and max_inputs == 1 %}
A single input is required.
{%- elif max_inputs > min_inputs %}
A minimum of {{ min_inputs }} and maximum of {{ max_inputs }} inputs are allowed.
{%- endif %}
{%- if params|length > 0 %}
Parameters:
{%- for key, value in params.items() %}
{{ key }}: {{ value }}
{%- endfor %}
{%- endif %}
"""


template = Environment(loader=BaseLoader).from_string(BASE_ABSTRACT)


def render_abstract(description, min_inputs=1, max_inputs=1, **params):
    """ Renders an abstract for a process.

    This function will use a jinja2 template and render out an abstract for a process. The keyword arguments
    for the ``func`` function are used to enable details in the abstract.

    Args:
        description (str): The process description.
        func (function): The process function.
        template (jinja2.Template): The jinja2 template that will be used to render the abstract.

    Returns:
        str: The abstract as a string.
    """
    kwargs = {
        'description': description,
        'min_inputs': min_inputs,
        'max_inputs': max_inputs,
        'params': params,
    }

    return template.render(**kwargs)


AXES = 'A list of axes to reduce dimensionality over. Separate multiple values with "|" e.g. time|lat.'
CONST = 'A float value that will be applied element-wise.'
COND = 'A condition that when true will preserve the value, otherwise the value will be set to "nan".'
FILLNA = 'A float value to replace "nan" values."'
VARIABLE = 'The variable to process.'
BINS = 'A list of bins. Separate values with "|" e.g. 0|10|20, this would create 2 bins (0-10), (10, 20).'

WHERE_ABS = """Filters elements based on a condition.

Supported comparisons: >, >=, <, <=, ==, !=

Left hand side should be a variable or axis name and the right can be an int or float.

Examples:
    lon>180
    pr>0.000023408767
"""

ABSTRACT_MAP = {
    'CDAT.abs': render_abstract('Computes element-wise absolute value.'),
    'CDAT.add': render_abstract('Adds two variables or a constant element-wise.', const=CONST, max_inputs=2),
    'CDAT.aggregate': render_abstract('Aggregates a variable spanning two or more files.', max_inputs=float('inf')),
    'CDAT.divide': render_abstract('Divides a variable by another or a constant element-wise.', const=CONST, max_inputs=2),
    'CDAT.exp': render_abstract('Computes element-wise exponential value.'),
    'CDAT.log': render_abstract('Computes element-wise log value.'),
    'CDAT.max': render_abstract('Computes the maximum value over one or more axes.', axes=AXES),
    'CDAT.mean': render_abstract('Computes the mean over one or more axes.', axes=AXES),
    'CDAT.min': render_abstract('Computes the minimum value over one or more axes.', axes=AXES),
    'CDAT.multiply': render_abstract('Multiplies a variable by another or a constant element-wise', const=CONST, max_inputs=2),
    'CDAT.power': render_abstract('Takes a variable to the power of another variable or a constant element-wise.', const=CONST),
    'CDAT.subset': render_abstract('Computes the subset of a variable defined by a domain.'),
    'CDAT.subtract': render_abstract('Subtracts a variable from another or a constant element-wise.', const=CONST, max_inputs=2),
    'CDAT.sum': render_abstract('Computes the sum over one or more axes.', axes=AXES),
    'CDAT.merge': render_abstract('Merges variable from second input into first.', min_inputs=2, max_inputs=float('inf')),
    'CDAT.where': render_abstract(WHERE_ABS, cond=COND, fillna=FILLNA),
    'CDAT.groupby_bins': render_abstract('Groups values of a variable into bins.', variable=VARIABLE, bins=BINS),
}


def process_wrapper(self, context):
    """ Wrapper function for a process.

    This function acts as the main entrypoint of a Celery task. It represents a single process e.g. Subset, Aggregate,
    etc. The function calls ``workflow_func`` since a single process is just a workflow with a single task.

    Args:
        context (OperationContext): The OperationContext holding all details of the current job.
    """
    return workflow_func(context)


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

    # Use jinja2 to template process abstract
    template = Environment(loader=BaseLoader).from_string(BASE_ABSTRACT)

    logger.info('Loadiung jinja2 abstract tempalate')

    for name, func in PROCESS_FUNC_MAP.items():
        module, operation = name.split('.')

        # Create a unique function for each process
        p = copy_function(process_wrapper, operation)

        # Decorate the new function as a Celery task
        shared = base.cwt_shared_task()(p)

        abstract = ABSTRACT_MAP[name]

        # Decorate the Celery task as a registered process
        register = base.register_process(module, operation, abstract=abstract)(shared)

        # Bind the new function to the "cdat" module
        setattr(cdat, p.__name__, register)

        logger.info('Binding process %r', name)

    return base.REGISTRY.values()
