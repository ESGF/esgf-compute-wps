#! /usr/bin/env python

import os
from functools import partial

import cdms2
import cwt
import dask
import dask.array as da
import cdms2
from celery.utils.log import get_task_logger
from dask.distributed import Client
from distributed.diagnostics.progressbar import ProgressBar
from distributed.utils import LoopRunner
from distributed.client import futures_of
from tornado.ioloop import IOLoop

from compute_tasks import base
from compute_tasks import context as ctx
from compute_tasks import managers
from compute_tasks import WPSError
from compute_tasks import DaskClusterAccessError
from compute_tasks.dask_serialize import regrid_chunk

logger = get_task_logger('compute_tasks.cdat')

DEV = os.environ.get('DEV', False)

NAMESPACE = os.environ.get('NAMESPACE', 'default')


class DaskJobTracker(ProgressBar):
    def __init__(self, context, futures, scheduler=None, interval='100ms', complete=True):
        futures = futures_of(futures)

        if not isinstance(futures, (list, set)):
            futures = [futures, ]

        context.message('Tracking {!r} futures', len(futures))

        super(DaskJobTracker, self).__init__(futures, scheduler, interval, complete)

        self.context = context
        self.last = None

        self.loop = IOLoop()

        loop_runner = LoopRunner(self.loop)
        loop_runner.run_sync(self.listen)

    def _draw_bar(self, **kwargs):
        logger.info('_draw_bar %r', kwargs)

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


WORKFLOW_ABSTRACT = """
This operation is used to store global values in workflows. Domain, regridders and parameters defined
here will become the default values on child operations.
"""


@base.register_process('CDAT', 'workflow', abstract=WORKFLOW_ABSTRACT, inputs='*')
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
    fm = managers.FileManager(context)

    # Topologically sort the operations
    topo_order = context.topo_sort()

    # Hold the intermediate inputs
    interm = {}

    while topo_order:
        next = topo_order.pop(0)

        context.message('Processing operation {!r} - {!r}', next.name, next.identifier)

        # if all inputs are variables then create a new InputManager
        if all(isinstance(x, cwt.Variable) for x in next.inputs):
            input = managers.InputManager.from_cwt_variables(fm, next.inputs)

            # Subset the inputs
            src, subset = input.subset(next.domain)

            context.track_src_bytes(src.nbytes)

            context.track_in_bytes(subset.nbytes)

            # If the current process is in the function map then apply the process
            if next.identifier in PROCESS_FUNC_MAP:
                output = PROCESS_FUNC_MAP[next.identifier](next, *[input, ])
            else:
                output = input

            # Regrid to the target grid
            if next.gridder is not None:
                regrid(next, output)

            # Add to intermediate store
            interm[next.name] = output
        else:
            # Grab all the intermediate inputs
            try:
                inputs = [interm[x.name] for x in next.inputs]
            except KeyError as e:
                raise WPSError('Unable to find intermidiate {!s}', e)

            # Create copy of single input, multiple inputs automatically create a new output
            if len(inputs) == 1:
                inputs = [inputs[0].copy(), ]

            try:
                output = PROCESS_FUNC_MAP[next.identifier](next, *inputs)
            except KeyError as e:
                raise WPSError('Operation {!s} is not supported in a workflow', e)
            except Exception:
                raise

            # Regrid to target grid
            if next.gridder is not None:
                regrid(next, output)

            # Add new input to intermediate store
            interm[next.name] = output

        context.message('Storing intermediate {!r}', next.name)

    context.message('Preparing to execute workflow')

    # Initialize the cluster resources
    try:
        client = Client('{!s}.{!s}.svc:8786'.format(context.extra['DASK_SCHEDULER'], NAMESPACE))
    except OSError:
        raise DaskClusterAccessError()

    try:
        delayed = []

        for output in context.output_ops():
            try:
                interm_value = interm.pop(output.name)
            except KeyError as e:
                raise WPSError('Did not find intermediate %s', e)

            context.track_out_bytes(interm_value.nbytes)

            # Create an output xarray Dataset
            dataset = interm_value.to_xarray()

            output_name = '{!s}-{!s}'.format(output.name, output.identifier)

            local_path = context.build_output_variable(interm_value.var_name, name=output_name)

            context.message('Building output for {!r} - {!r}', output.name, output.identifier)

            logger.debug('Writing local output to %r', local_path)

            # Create an output file and store the future
            delayed.append(dataset.to_netcdf(local_path, compute=False))

        # Should be refactored with the above
        for output in context.interm_ops():
            try:
                interm_value = interm.pop(output.name)
            except KeyError as e:
                raise WPSError('Did not find intermediate %s', e)

            # Create an output xarray Dataset
            dataset = interm_value.to_xarray()

            output_name = '{!s}-{!s}'.format(output.name, output.identifier)

            local_path = context.build_output_variable(interm_value.var_name, name=output_name)
            # local_path = context.build_output_variable(interm[output.name].var_name, name=output_name)

            context.message('Building output for {!r} - {!r}', output.name, output.identifier)

            logger.debug('Writing local output to %r', local_path)

            # Create an output file and store the future
            delayed.append(dataset.to_netcdf(local_path, compute=False))

        context.message('Executing workflow')

        with ctx.ProcessTimer(context):
            # Execute the futures
            fut = client.compute(delayed)

            # Track the progress
            DaskJobTracker(context, fut)
    except Exception as e:
        raise WPSError('Error executing process: {!r}', e)

    return context


def process(context, process_func=None, aggregate=False):
    """ Process a single variable.

    Initialize the cluster workers, create the dask graph then execute.

    The `process_func` function should take a cwt.Process and a list of
    compute_tasks.managers.FileManager. See process_input as an example.

    Args:
        context: A cwt.context.OperationContext.
        process_func: A custom processing function to be applied.
        aggregate: A bool value to treat the inputs as an aggregation.

    Returns:
        An updated cwt.context.OperationContext.
    """
    try:
        client = Client('{!s}.{!s}.svc:8786'.format(context.extra['DASK_SCHEDULER'], NAMESPACE))
    except OSError:
        raise DaskClusterAccessError()

    fm = managers.FileManager(context)

    # Create the initial list of inputs
    if aggregate:
        inputs = [managers.InputManager.from_cwt_variables(fm, context.inputs), ]

        context.message('Building aggregation with {!r} inputs', len(context.inputs))
    else:
        inputs = [managers.InputManager.from_cwt_variable(fm, x) for x in context.inputs]

        context.message('Building {!r} inputs', len(inputs))

    # Subset each input and track the bytes
    for input in inputs:
        src, subset = input.subset(context.domain)

        context.track_src_bytes(src.nbytes)

        context.track_in_bytes(subset.nbytes)

        context.message('Data subset shape {!r}', input.shape)

    # Apply processing if needed
    if process_func is not None:
        output = process_func(context.operation, *inputs)

        context.message('Applied process {!r} shape {!r}', context.operation.identifier, output.shape)
    else:
        output = inputs[0]

    # Apply regridding if needed
    if context.is_regrid:
        regrid(context.operation, output)

        context.message('Applied regridding shape {!r}', output.shape)

    context.message('Preparing to execute')

    context.track_out_bytes(output.nbytes)

    dataset = output.to_xarray()

    local_path = context.build_output_variable(output.var_name)

    logger.debug('Writing output to %r', local_path)

    try:
        # Execute the dask graph
        delayed = dataset.to_netcdf(local_path, compute=False)

        context.message('Executing')

        with ctx.ProcessTimer(context):
            fut = client.compute(delayed)

            DaskJobTracker(context, fut)
    except Exception as e:
        raise WPSError('Error executing process: {!r}', e)

    return context


def regrid(operation, *inputs):
    """ Build regridding from dask delayed.

    Converts the current dask array into an array of delayed functions. Next for each delayed function.
    another delayed function is linked. This function will regrid the chunk of data. Next the new delayed
    functions are converted back to dask arrays which are then concatenated back together. Lastly the
    spatial axes and bounds are updated.

    Args:
        operation: A cwt.Process instance.
        inputs: A list of compute_tasks.managers.FileManager instances.
    """
    gridder = operation.get_parameter('gridder', True)

    input = inputs[0]

    # Build a selector which is a dict mapping axis name to the desired range
    selector = dict((x, (input.axes[x][0], input.axes[x][-1])) for x in input.variable_axes)

    # Generate the target grid
    grid, tool, method = input.regrid_context(gridder, selector)

    # Convert to delayed functions
    delayed = input.variable.to_delayed().squeeze()

    # Flatten the array
    if delayed.size == 1:
        delayed = delayed.reshape((1, ))

    logger.debug('Converted dask graph to %r delayed', delayed.shape)

    # Create list of axis data that is present in the selector.
    axis_data = [input.axes[x] for x in selector.keys()]

    logger.info('Creating regrid_chunk delayed functions')

    # Build list of delayed functions that will process the chunks
    regrid_delayed = [dask.delayed(regrid_chunk)(x, axis_data, grid, tool, method) for x in delayed]

    logger.info('Converting delayed functions to dask arrays')

    # Build a list of dask arrays created by the delayed functions
    regrid_arrays = [da.from_delayed(x, y.shape[:-2] + grid.shape, input.dtype)
                     for x, y in zip(regrid_delayed, input.blocks)]

    # Concatenated the arrays together
    input.variable = da.concatenate(regrid_arrays)

    logger.info('Concatenated %r arrays to output %r', len(regrid_arrays), input.variable)

    input.axes['lat'] = grid.getLatitude()

    input.vars['lat_bnds'] = cdms2.createVariable(
        input.axes['lat'].getBounds(),
        id='lat',
        axes=[input.axes[x] for x in input.vars_axes['lat_bnds']]
    )

    input.axes['lon'] = grid.getLongitude()

    input.vars['lon_bnds'] = cdms2.createVariable(
        input.axes['lon'].getBounds(),
        id='lon',
        axes=[input.axes[x] for x in input.vars_axes['lon_bnds']]
    )


FEAT_AXES = 'FEAT_AXES'
FEAT_CONST = 'FEAT_CONST'
FEAT_MULTI = 'FEAT_MULTI'


def process_input(operation, *inputs, process_func=None, **supported): # noqa E999
    """ Process inputs.

    Possible values for `supported`:
        single_input_axes: Process a single input array over some axes.
        single_input_constant: Process a single input array with a constant value.
        multiple_input: Process multiple inputs.

    Args:
        operation: A cwt.Process instance.
        inputs: A list of compute_tasks.managers.FileManager instances.
        process_func: An optional dask ufunc.
        supported: An optional dict defining the support processing types. See above for explanation.
    """
    axes = operation.get_parameter('axes')

    constant = operation.get_parameter('constant')

    logger.info('Axes %r Constant %r', axes, constant)

    if axes is not None:
        if not supported.get(FEAT_AXES, False):
            raise WPSError('Axes parameter is not supported by operation {!r}', operation.identifier)

        # Apply process to first input over axes
        output = process_single_input(axes.values, process_func, inputs[0])
    elif constant is not None:
        if not supported.get(FEAT_CONST, False):
            raise WPSError('Constant parameter is not supported by operation {!r}', operation.identifier)

        try:
            constant = float(constant.values[0])
        except ValueError:
            raise WPSError('Invalid constant value {!r} type {!s} expecting <class \'float\'>',
                           constant, type(constant))

        output = inputs[0]

        # Apply the process to the existing dask array
        output.variable = process_func(output.variable, constant)

        logger.info('Process output %r', output.variable)
    elif len(inputs) > 1:
        if not supported.get(FEAT_MULTI, False):
            raise WPSError('Multiple inputs are not supported by operation {!r}', operation.identifier)

        # Apply the process to all inputs
        output = process_multiple_input(process_func, *inputs)
    else:
        output = inputs[0]

        # Apply the process
        output.variable = process_func(output.variable)

        logger.info('Process output %r', output.variable)

    return output


def process_single_input(axes, process_func, input):
    """ Process single input.

    Args:
        axes: List of str axis names.
        process_func: A function with the same signature as process_input.
        input: A compute_tasks.managers.FileManager instance.

    Returns:
        A new instance of compute_tasks.managers.FileManager containing the results.
    """
    # Get present in the current data
    map_keys = list(input.variable_axes)

    # Reduce them from names to indices
    indices = tuple(map_keys.index(x) for x in axes)

    logger.info('Mapped axes %r to indices %r', axes, indices)

    # Apply the dask ufunc
    input.variable = process_func(input.variable, axis=indices)

    logger.info('Process output %r', input.variable)

    # Remove the axes that have been squashed
    for axis in axes:
        logger.info('Removing axis %r', axis)

        input.remove_axis(axis)

    return input


def process_multiple_input(process_func, input1, input2):
    """ Process multiple inputs.

    Note that some ufuncs will only process a single array over some dimensions,
    in this the function will need to be registed in the REQUIRES_STACK variable.

    Args:
        process_func: A function with the same signature as process_input.
        input1: A compute_tasks.managers.FileManager instance.
        input2: A compute_tasks.managers.FileManager instance.

    Returns:
        A new instance of compute_tasks.managers.FileManager containing the results.
    """
    # Create a copy to retain any metadata
    new_input = input1.copy()

    # Check if we need to stack the arrays.
    if process_func.__name__ in REQUIRES_STACK:
        stacked = da.stack([input1.variable, input2.variable])

        logger.info('Stacking inputs %r', stacked)

        new_input.variable = process_func(stacked, axis=0)

    else:
        new_input.variable = process_func(input1.variable, input2.variable)

    logger.info('Process output %r', new_input.variable)

    return new_input


REQUIRES_STACK = [
    da.sum.__name__,
    da.max.__name__,
    da.min.__name__,
    da.average.__name__,
]

DESCRIPTION_MAP = {
    'CDAT.abs': 'Computes the element-wise absolute value.',
    'CDAT.add': 'Adds an element-wise constant or another input.',
    'CDAT.aggregate': 'Aggregates multiple files over a temporal axis.',
    'CDAT.average': 'Computes the average over a set of axes or inputs.',
    'CDAT.divide': 'Divides element-wise by a constant or second input.',
    'CDAT.exp': 'Computes element-wise exponential.',
    'CDAT.log': 'Computes element-wise natural log',
    'CDAT.max': 'Computes the maximum over a set of axes, between a constant or a second input.',
    'CDAT.min': 'Computes the minimum over a set of axes, between a constant or a second input.',
    'CDAT.multiply': 'Multiplies element-wise by a constant or a second input.',
    'CDAT.power': 'Computes the element-wise power by a constant.',
    'CDAT.regrid': 'Regrids input by target grid.',
    'CDAT.subset': 'Subsets an input to desired domain.',
    'CDAT.subtract': 'Subtracts element-wise constant or a second input.',
    'CDAT.sum': 'Computes the sum over a set of axes.',
}

PROCESS_FUNC_MAP = {
    'CDAT.abs': partial(process_input, process_func=da.absolute),
    'CDAT.add': partial(process_input, process_func=da.add, FEAT_CONST=True, FEAT_MULTI=True),
    'CDAT.aggregate': None,
    'CDAT.average': partial(process_input, process_func=da.average, FEAT_MULTI=True),
    'CDAT.divide': partial(process_input, process_func=da.divide, FEAT_CONST=True, FEAT_MULTI=True),
    'CDAT.exp': partial(process_input, process_func=da.exp),
    'CDAT.log': partial(process_input, process_func=da.log),
    'CDAT.max': partial(process_input, process_func=da.max, FEAT_AXES=True, FEAT_CONST=True, FEAT_MULTI=True),
    'CDAT.min': partial(process_input, process_func=da.min, FEAT_AXES=True, FEAT_CONST=True, FEAT_MULTI=True),
    'CDAT.multiply': partial(process_input, process_func=da.multiply, FEAT_CONST=True, FEAT_MULTI=True),
    'CDAT.power': partial(process_input, process_func=da.power, FEAT_CONST=True),
    'CDAT.regrid': regrid,
    'CDAT.subset': None,
    'CDAT.subtract': partial(process_input, process_func=da.subtract, FEAT_CONST=True, FEAT_MULTI=True),
    'CDAT.sum': partial(process_input, process_func=da.sum, FEAT_AXES=True),
}


def render_abstract(description, func, template):
    axes = False
    const = False
    multi = False

    try:
        kwargs = func.keywords
    except AttributeError:
        pass
    else:
        axes = FEAT_AXES in kwargs and kwargs[FEAT_AXES]

        const = FEAT_CONST in kwargs and kwargs[FEAT_CONST]

        multi = FEAT_MULTI in kwargs and kwargs[FEAT_MULTI]

    return template.render(description=description, axes=axes, const=const, multi=multi)


BASE_ABSTRACT = """
{{ description }}
{%- if multi %}

Supports multiple inputs.
{%- endif %}
{%- if (axes or constant) %}

Optional parameters:
{%- if axes %}
    axes: A list of axes to operate on. Multiple values should be separated by "|" e.g. "lat|lon".
{%- endif %}
{%- if constant %}
    constant: An integer or float value that will be applied element-wise.
{%- endif %}
{%- endif %}
"""


def register_processes():
    from jinja2 import Environment, BaseLoader
    from compute_tasks import cdat

    template = Environment(loader=BaseLoader).from_string(BASE_ABSTRACT)

    for name, func in PROCESS_FUNC_MAP.items():
        module, operation = name.split('.')

        def process_wrapper(self, context):
            if context.identifier == 'CDAT.aggregate':
                return process(context, aggregate=True)
            else:
                func = PROCESS_FUNC_MAP[context.identifier]

                return process(context, process_func=func)

        process_wrapper.__name__ = '{!s}_func'.format(operation)

        shared = base.cwt_shared_task()(process_wrapper)

        abstract = render_abstract(DESCRIPTION_MAP[name], func, template)

        inputs = 1

        try:
            if FEAT_MULTI in func.keywords:
                inputs = 2
            elif name == 'CDAT.aggregate':
                inputs = '*'
        except AttributeError:
            pass

        register = base.register_process(module, operation, abstract=abstract, inputs=inputs)(shared)

        setattr(cdat, process_wrapper.__name__, register)


register_processes()
