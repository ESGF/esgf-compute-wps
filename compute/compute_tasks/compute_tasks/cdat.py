#! /usr/bin/env python

from functools import partial
import json
import math
import os
import re
import time
from urllib.parse import urlparse
import uuid

from celery.utils.log import get_task_logger
import cwt
from dask.distributed import Client
from distributed.client import futures_of
from distributed.diagnostics.progressbar import ProgressBar
from distributed.utils import LoopRunner
import metpy  # noqa
import numpy as np
import pandas as pd
from tornado.ioloop import IOLoop
import xarray as xr
import zarr

from compute_tasks import base
from compute_tasks import celery_app
from compute_tasks import metrics
from compute_tasks import utilities
from compute_tasks import WPSError

logger = get_task_logger("compute_tasks.cdat")


XARRAY_OPEN_KWARGS = {
    "engine": "netcdf4",
    "chunks": {
        "time": 100,
    },
    "decode_times": False,
}


class DaskTimeoutError(WPSError):
    pass


class DaskTaskTracker(ProgressBar):
    def __init__(
        self,
        context,
        futures,
        scheduler=None,
        interval="100ms",
        complete=True,
    ):
        """Init method.

        Class init method.

        Args:
            context (context.OperationContext): The current context.
            futures (list): A list of futures to track the progress of.
            scheduler (dask.distributed.Scheduler, optional): The dask
                scheduler being used.
            interval (str, optional): The interval used between posting updates
                of the tracked futures.
            complete (bool, optional): Whether to callback after all futures
                are complete.
        """
        self.futures = futures_of(futures)

        context.message(f"Tracking {len(self.futures)!r} futures")

        super(DaskTaskTracker, self).__init__(
            self.futures, scheduler, interval, complete
        )

        self.context = context
        self.last = None
        self.updated = None

        self.loop = IOLoop()

        loop_runner = LoopRunner(self.loop)
        loop_runner.run_sync(self.listen)

    def _draw_bar(self, **kwargs):
        """Update callback.

        After each interval this method is called allowing for a update of the
        progress.

        Args:
            remaining (int): The number of remaining futures to be processed.
            all (int): The total number of futures to be processed.
        """
        logger.debug("_draw_bar %r", kwargs)

        remaining = kwargs.get("remaining", 0)

        all = kwargs.get("all", None)

        frac = (1 - remaining / all) if all else 1.0

        percent = int(100 * frac)

        logger.debug("Percent processed %r", percent)

        if self.last is None or self.last != percent:
            self.updated = time.time()

            self.last = percent

            self.context.message("Processing", percent=percent)
        else:
            update_timeout = int(os.environ.get("UPDATE_TIMEOUT", 120))

            # Check how long since last update
            if time.time() > (self.updated + update_timeout):
                logger.error(
                    f"Job has not progressed in {update_timeout} seconds"
                )

                raise DaskTimeoutError(
                    f"Job has not progresed in {update_timeout} seconds"
                )

    def _draw_stop(self, **kwargs):
        pass


def clean_output(dataset):
    """Cleans the encoding for a variable.

    Sometimes a variable's encoding will have missing_value and _FillValue,
    xarray does not like this, thus one must be removed. ``missing_value``
    was arbitrarily selected to be removed.

    Args:
        dataset (xarray.DataSet): The dataset to check each variables encoding
            settings.
    """
    for name, variable in dataset.variables.items():
        if (
            "missing_value" in variable.encoding
            and "_FillValue" in variable.encoding
        ):
            del variable.encoding["missing_value"]

    for x in dataset.coords:
        if dataset.coords[x].dtype == np.object:
            dataset.coords[x] = [str(y) for y in dataset.coords[x].values]

    return dataset


def find_most_verbose_time(t1, t2):
    """Finds verbose time format.

    Takes two datetime objects and finds the minimal format to differentiate
    the two values.

    Args:
        t1 (datetime): First time value.
        t2 (datetime): Second time value.

    Returns:
        str: Minimal format to differentiate time values or None if not
            possible.
    """
    formats = (
        "%Y",
        "%Y-%m",
        "%Y-%m-%d",
        "%Y-%d-%m-%dT%H",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    )

    for f in formats:
        t1_fmt = pd.to_datetime(str(t1)).strftime(f)

        t2_fmt = pd.to_datetime(str(t2)).strftime(f)

        if t1_fmt != t2_fmt:
            return f

    return None


def build_filename(ds, operation, index=None):
    uid = str(uuid.uuid4())[:8]

    desc = ""
    part = ""

    if "time" in ds:
        start, stop = ds.time[0].values, ds.time[-1].values

        if start.dtype == np.float:
            desc = f"_{start!s}-{stop!s}"
        else:
            fmt = find_most_verbose_time(start, stop)

            if fmt is not None:
                t1 = pd.to_datetime(str(start)).strftime(fmt)

                t2 = pd.to_datetime(str(stop)).strftime(fmt)

                desc = f"_{t1!s}-{t2!s}"

    if index is not None:
        part = f"_{index}"

    return f"{operation.identifier}_{operation.name}_{uid}{desc}{part}.nc"


def generate_chunk_indices(variable, ds, max_file_size):
    number_of_files = math.ceil(ds.nbytes / max_file_size)

    logger.info(f"Splitting output into {number_of_files} files")

    dim_sizes = dict((x, len(ds.coords[x])) for x in ds.coords)

    split_dim, dim_size = max(dim_sizes.items(), key=lambda x: x[1])

    logger.info(
        f"Splitting along {split_dim!r} dimension of {dim_size} length"
    )

    items_per_file = math.ceil(dim_size / number_of_files)

    chunk_indices = [
        slice(x, min(x + items_per_file, dim_size))
        for x in range(0, dim_size, items_per_file)
    ]

    return split_dim, chunk_indices


def build_split_output(
    context, variables, interm_ds, output, output_name, max_size
):
    # Choose arbitary first variable, api doesn't support multiple
    variable = variables[0]

    chunk_dim, chunk_indices = generate_chunk_indices(
        variable, interm_ds, max_size
    )

    datasets = [interm_ds.isel({chunk_dim: x}) for x in chunk_indices]

    filenames = [
        build_filename(interm_ds, output, i) for i, _ in enumerate(datasets)
    ]

    local_paths = [
        context.build_output(
            "application/netcdf",
            filename=x,
            var_name=variable,
            name=output_name,
        )
        for x in filenames
    ]

    for x in datasets:
        clean_output(x)

        context.set_provenance(x)

    delayed = xr.save_mfdataset(
        datasets,
        local_paths,
        compute=False,
        format="NETCDF3_64BIT",
        engine="netcdf4",
    )

    return delayed


def build_output(context, variables, interm_ds, output, output_name):
    filename = build_filename(interm_ds, output)

    # Choose arbitary first variable, api doesn't support multiple
    variable = variables[0]

    local_path = context.build_output(
        "application/netcdf",
        filename=filename,
        var_name=variable,
        name=output_name,
    )

    logger.debug("Writing local output to %r", local_path)

    interm_ds = clean_output(interm_ds)

    context.set_provenance(interm_ds)

    delayed = interm_ds.to_netcdf(
        local_path, compute=False, format="NETCDF3_64BIT", engine="netcdf4"
    )

    context.message(f"Setting output filename {filename}")

    return delayed


def gather_workflow_outputs(context, interm, operations):
    """Gather the ouputs for a set of processes (operations).

    For each process we find it's output. Next the output path for the file is
    created. Next the Dask delayed function to create a netCDF file is created
    and added to a list that is returned.

    Args:
        context (context.OperationContext): The current context.
        interm (dict): A dict mapping process names to Dask delayed functions.
        operations (list): A list of ``cwt.Process`` objects whose outputs are
            being gathered.

    Returns:
        list: Of Dask delayed functions outputting netCDF files.

    Raises:
        WPSError: If output is not found or there exists and issue creating the
            output netCDF file.
    """
    delayed = []

    for output in operations:
        context.message(
            f"Building output for {output.name!r} - {output.identifier!r}"
        )

        try:
            interm_ds = interm.pop(output.name)
        except KeyError as e:
            raise WPSError("Failed to find intermediate {!s}", e)

        output_name = "{!s}-{!s}".format(output.name, output.identifier)

        variables = context.input_var_names[output.name]

        # Limit max filesize to 100MB
        max_size = 1024e5

        try:
            if interm_ds.nbytes > max_size:
                _delayed = build_split_output(
                    context,
                    variables,
                    interm_ds,
                    output,
                    output_name,
                    max_size,
                )
            else:
                _delayed = build_output(
                    context, variables, interm_ds, output, output_name
                )
        except ValueError:
            shapes = dict(
                (x, y.shape[0] if len(y.shape) > 0 else None)
                for x, y in interm_ds.coords.items()
            )

            bad_coords = ", ".join([x for x, y in shapes.items() if y == 1])

            raise WPSError(
                "Domain resulted in empty coordinates {!s}", bad_coords
            )

        delayed.append(_delayed)

    return delayed


def input_nbytes(input):
    if isinstance(input, xr.core.groupby.DatasetGroupBy):
        nbytes = sum(y.nbytes for x, y in input)
    elif isinstance(input, xr.Dataset):
        nbytes = input.nbytes
    else:
        raise WPSError(f"Could not determine nbytes for {type(input)}")

    return nbytes


def subset_dataset(context, domain, source, method=None, ignore_step=False, **kwargs):
    if domain is None:
        return source

    initial_shapes = dict((x, y.shape) for x, y in source.coords.items())

    for name, dim in domain.dimensions.items():
        if dim.start == dim.end:
            selector = {name: dim.start}
        else:
            selector = {
                name: slice(
                    dim.start,
                    dim.end,
                    dim.step if not ignore_step else 1,
                )
            }

        if dim.crs == cwt.INDICES:
            source = source.isel(selector)
        elif dim.crs == cwt.VALUES:
            source = source.sel(selector, method=method)
        elif dim.crs == cwt.TIMESTAMPS:
            source = xr.decode_cf(source).sel(selector, method=method)
        else:
            raise WPSError(f"Could not handle CRS {str(dim.crs)!r}")

        before = initial_shapes[name]
        after = source.coords[name].shape

        context.message(f"Subset {name!r} {before!r} -> {after!r}")

    for name, shape in source.dims.items():
        if shape == 0:
            raise WPSError(f"Subset of {name!r} resulted in zero length"
                           " dimension")

    return source


def get_drop_variables(source):
    all_vars = set(source.variables.keys())

    keep_vars = set([
        x
        for x, y in source.variables.items()
        if "time" in y.dims and x != "time_bnds"
    ])

    return list(all_vars-keep_vars)


def write_cache(context, source, domain, key, drop_vars, attrs, **kwargs):
    subset = subset_dataset(context, domain, source, ignore_step=True, **kwargs)

    if "time" not in subset.indexes:
        return source

    subset_index = subset.get_index("time")

    start, stop = subset_index[0], subset_index[-1]

    if isinstance(subset_index, xr.CFTimeIndex):
        source_index = xr.decode_cf(source).get_index("time")

        region_slice = source_index.slice_indexer(start, stop)
    else:
        region_slice = source.get_index("time").slice_indexer(start, stop)

    cached_slices = celery_app.decoder(attrs.get("INTERVALS", "[]"))

    cached_slices.append(region_slice)

    attrs["INTERVALS"] = celery_app.encoder(cached_slices)

    region = {
        "time": region_slice,
    }

    logger.info(f"Writing region {region!r} to cache")

    kwargs = {
        "group": key,
        "compute": False,
        "region": region,
    }

    delayed = subset.drop_vars(drop_vars).to_zarr(context.store, **kwargs)

    context.add_delayed(delayed)

    return subset


def load_cached_index(attrs):
    interval_slices = celery_app.decoder(attrs["INTERVALS"])

    interval_indexes = [
        pd.Float64Index(pd.RangeIndex(x.start, x.stop))
        for x in interval_slices
    ]

    index = interval_indexes[0]

    if len(interval_indexes) > 1:
        for x in interval_indexes[1:]:
            index = index.union(x)

    return index


def split_intervals(index):
    values = index.tolist()

    diff = np.ediff1d(values)

    split_index = np.argwhere(diff>1).squeeze().tolist()

    logger.info(f"Split index {split_index!r}")

    new_indexes = []

    if len(index) > 0:
        split_index.insert(0, 0)

        split_index.append(len(index))

        for x, y in zip(split_index, split_index[1:]):
            new_indexes.append(index[x:y])

    return new_indexes


def missing_intervals(context, subset_index, cached_index, source, subset, key,
                      domain, drop_vars, attrs, **kwargs):
    diff = subset_index.difference(cached_index)

    diff = split_intervals(diff)

    logger.info(f"Difference {diff!r}")

    old_time = domain.get_dimension("time")
    step = 1 if old_time is None else old_time.step

    remote = []

    for x in diff:
        time = cwt.Dimension("time", int(x[0]), int(x[-1])+1, cwt.INDICES, step)

        domain.dimensions["time"] = time

        interval = write_cache(context, source, domain, key, drop_vars, attrs,
                    **kwargs)

        remote.append(interval)

    return remote


def cached_intervals(context, subset_index, cached_index, key, domain, **kwargs):
    inter = subset_index.intersection(cached_index)

    inter = split_intervals(inter)

    logger.info(f"Intersection {inter!r}")

    old_time = domain.get_dimension("time")
    step = 1 if old_time is None else old_time.step

    zarr_kwargs = {
        "group": key,
        "decode_times": False,
    }

    cached = []
    cached_data = xr.open_zarr(context.store, **zarr_kwargs)

    for x in inter:
        time = cwt.Dimension("time", int(x[0]), int(x[-1])+1, cwt.INDICES, step)

        domain.dimensions["time"] = time

        interval = subset_dataset(context, domain, cached_data, **kwargs)

        cached.append(interval)

    return cached


def try_cache(context, source, subset, key, domain, **kwargs):
    attrs_key = f"{key}/.zattrs"
    attrs = zarr.attrs.Attributes(context.store, attrs_key)

    drop_vars = get_drop_variables(subset)

    if zarr.storage.contains_group(context.store, key):
        logger.info(f"Cache hit {key!r}")

        subset_index = pd.Float64Index(
            source.get_index("time").get_indexer(subset.get_index("time"))
        )

        cached_index = load_cached_index(attrs)

        logger.info(f"Cached index {cached_index!r}")

        remote = missing_intervals(context, subset_index, cached_index, source, subset,
                                   key, domain, drop_vars, attrs, **kwargs)

        cached = cached_intervals(context, subset_index, cached_index, key, domain,
                                  **kwargs)

        sorted_data = sorted(remote+cached, key=lambda x: x.time[0])

        subset = xr.concat(sorted_data, "time")
    else:
        logger.info(f"Cache miss {key!r}")

        zarr_kwargs = {
            "group": key,
            "compute": False,
        }

        source.drop_vars(drop_vars).to_zarr(context.store, **zarr_kwargs)

        write_cache(context, source, domain, key, drop_vars, attrs, **kwargs)

    return subset


def process_subset(context, operation, source, *ignored, **kwargs):
    """Subsets a Dataset.

    Subset a ``xarray.Dataset`` using the user-defined ``cwt.Domain``. For
    each dimension the selector is built and applied using the appropriate
    method.

    Args:
        context (context.OperationContext): The current operation context.
        operation (cwt.Process): The process definition the input is
            associated with.
        source (cwt.Variable): Input to subset.
        *ignored (List): List of extra inputs that will be ignored.
        **kwargs (Dict): Extra configuration.
    """
    ds = xr.open_dataset(source.uri, **XARRAY_OPEN_KWARGS)

    subset = subset_dataset(context, operation.domain, ds, **kwargs)

    key = urlparse(source.uri).path.split("/")[-1]

    try:
        subset = try_cache(context, ds, subset, key, operation.domain, **kwargs)
    except Exception:
        logger.exception()

        context.message(f"Caching error {key!r}")

    return subset


def post_processing(
    context, variable, output, rename=None, fillna=None, **kwargs
):
    logger.info(
        "Post-processing arguments rename %r fillna %r **kwargs %r",
        rename,
        fillna,
        kwargs,
    )

    # Default to first variable
    if variable is not None and isinstance(variable, (list, tuple)):
        variable = variable[0]

    if fillna is not None:
        context.message(f"Filling Nan with {fillna!r}")

        if variable is None:
            output = output.fillna(fillna)
        else:
            output[variable] = output[variable].fillna(fillna)

    if rename is not None:
        rename_dict = dict(x for x in zip(rename[::2], rename[1::2]))

        context.message(f"Renaming variables with mapping {rename_dict!r}")

        output = output.rename(rename_dict)

    return output


def process_dataset(context, operation, *input, func, variable, **kwargs):
    is_groupby = isinstance(input[0], xr.core.groupby.DatasetGroupBy)

    if is_groupby:
        output = func(input[0])
    else:
        if variable is None:
            output = func(input[0])
        else:
            output = input[0].copy()

            output[variable] = func(input[0][variable])

    output = post_processing(context, variable, output, **kwargs)

    return output


def process_reduce(
    context, operation, *input, func, axes, variable, **kwargs
):
    is_groupby = isinstance(input[0], xr.core.groupby.DatasetGroupBy)

    if is_groupby:
        output = func(input[0], None)
    else:
        if variable is None:
            output = func(input[0], axes)
        else:
            output = input[0].copy()

            output[variable] = func(input[0][variable], axes)

    output = post_processing(context, variable, output, **kwargs)

    return output


def process_dataset_or_const(
    context, operation, *input, func, variable, const, **kwargs
):
    if isinstance(input[0], xr.core.groupby.DatasetGroupBy):
        raise WPSError(
            "GroupBy input not supported for process {!s}",
            operation.identifier,
        )

    if const is None:
        if len(input) != 2:
            raise WPSError(
                'Process {!s} requires 2 inputs or "const" parameter.',
                operation.identifier,
            )

        if variable is None:
            output = func(input[0], input[1])
        else:
            output = input[0].copy()

            # Grab relative variable from its input
            inputs = [input[x][variable] for x in range(len(input))]

            output[variable] = func(*inputs)
    else:
        if variable is None:
            output = func(input[0], const)
        else:
            output = input[0].copy()

            output[variable] = func(input[0][variable], const)

    output = post_processing(context, variable, output, **kwargs)

    return output


def process_merge(context, operation, *input, compat, **kwargs):
    if compat is None:
        compat = "no_conflicts"

    if compat not in ("no_conflicts", "override"):
        raise WPSError(
            (
                "Cannot use {!r} as compat method, choose no_conflicts or"
                " override."
            )
        )

    input = list(input)

    context.message(
        f"Merging {len(input)!s} variable using compat {compat!r}"
    )

    return xr.merge(input, compat=compat)


def parse_condition(context, cond):
    if cond is None:
        raise WPSError('Missing parameter "cond"')

    p = r'(?P<left>\w+)\ ?(?P<comp>[<>=!]{1,2}|(is|not)null)(?P<right>-?\d+\.?\d?)?'

    match = re.match(p, cond)

    if match is None:
        raise WPSError("Condition is not valid, check abstract for format")

    comp = match["comp"]

    left = match["left"]

    try:
        right = float(match["right"])
    except ValueError:
        raise WPSError(
            "Error converting right argument {!s} {!s}",
            match["right"],
            type(match["right"]),
        )
    except TypeError:
        right = None

    context.message(f"Using condition {left!r} - {comp!r} - {right!r}")

    return left, comp, right


def build_condition(context, cond, input):
    left, comp, right = parse_condition(context, cond)

    if left not in input:
        raise WPSError("Did not find {!s} in input", left)

    input_item = input[left]

    if comp == ">":
        cond = input_item > right
    elif comp == ">=":
        cond = input_item >= right
    elif comp == "<":
        cond = input_item < right
    elif comp == "<=":
        cond = input_item <= right
    elif comp == "==":
        cond = input_item == right
    elif comp == "!=":
        cond = input_item != right
    elif comp in ("isnull", "notnull"):
        cond = getattr(input_item, comp)()
    else:
        raise WPSError("Comparison with {!s} is not supported", comp)

    return cond


def process_where(
    context, operation, *input, variable, cond, other, **kwargs
):
    if isinstance(input[0], xr.core.groupby.DatasetGroupBy):
        raise WPSError("Cannot apply where to DatasetGroupBy input.")

    cond = build_condition(context, cond, input[0])

    if other is None:
        other = xr.core.dtypes.NA

    if variable is None:
        output = input[0].where(cond, other)
    else:
        output = input[0].copy()

        output[variable] = input[0][variable].where(cond, other)

    output = post_processing(context, variable, output, **kwargs)

    return output


def process_groupby_bins(
    context, operation, *input, variable, bins, **kwargs
):
    context.message(f"Grouping {variable} into bins {bins!r}")

    try:
        output = input[0].groupby_bins(variable, bins)
    except ValueError:
        raise WPSError("Invalid bin value.")

    return output


def process_aggregate(context, operation, *input, **kwargs):
    context.message(f"Aggregating {len(input)} files by coords")

    variable = list(set([x.var_name for x in operation.inputs]))[0]

    isizes = [x[variable].shape for x in input]

    logger.info(f"{len(isizes)} inputs with shapes {isizes}")

    output = xr.combine_by_coords(input, combine_attrs="override")

    osize = output[variable].shape

    logger.info(f"Output shape {osize}")

    output = post_processing(context, variable, output, **kwargs)

    return output


def process_filter_map(
    context, operation, *input, variable, cond, other, func, **kwargs
):
    def _inner(x, cond, other, func):
        filtered = x.where(cond, other)

        return getattr(filtered, func)()

    if func is None:
        raise WPSError("Missing func parameter.")

    if isinstance(input[0], xr.core.groupby.DatasetGroupBy):
        cond = build_condition(context, cond, input[0]._obj)
    else:
        cond = build_condition(context, cond, input[0])

    if other is None:
        other = xr.core.dtypes.NA

    output = input[0].map(_inner, args=(cond, other, func))

    output = post_processing(context, variable, output, **kwargs)

    return output


def validate_pairs(num_param, **kwargs):
    logger.info(f"Validating pairs for {num_param} parameters")

    if num_param % 2 != 0:
        raise base.ValidationError("Expected even number of values.")

    return True


def validate_variable(values, input_var_names):
    logger.info(f"Validating variable {values[0]} in {input_var_names}")

    if values[0] not in input_var_names:
        raise base.ValidationError(
            f"Did not find variable {values[0]!r} in available inputs "
            f"{input_var_names!r}"
        )

    return True


param_variable = base.build_parameter(
    "variable",
    "Target variable for the process.",
    str,
    min=1,
    max=1,
    validate_func=validate_variable,
)
param_rename = base.build_parameter(
    "rename",
    (
        "List of pairs mapping variable to new name e.g. pr,pr_test will "
        "rename pr to pr_test."
    ),
    list,
    str,
    min=2,
    max=float("inf"),
    validate_func=validate_pairs,
)
param_fillna = base.build_parameter(
    "fillna", "The number used to replace nan values in output.", float
)

DEFAULT_PARAMS = [
    param_variable,
    param_rename,
    param_fillna,
]


def param_defaults(*ignore):
    def _wrapper(func):
        for x in DEFAULT_PARAMS:
            name = x["name"]
            if name not in ignore:
                if name in func._parameters:
                    raise Exception(
                        "Parameter {!s} already exists".format(name)
                    )

                func._parameters[name] = x

        return func

    return _wrapper


def bind_process_func(process_func):
    def _wrapper(func):
        func._process_func = process_func

        return func

    return _wrapper


param_axes = base.parameter(
    "axes", "A list of axes used to reduce dimensionality.", list, str
)
param_cond = base.parameter(
    "cond", "A condition that when true will preserve the value.", str
)
param_const = base.parameter(
    "const", "A value that will be applied element-wise.", float
)
param_other = base.parameter(
    "other", "A value that will be used when `cond` is false.", float
)


@bind_process_func(None)
@base.register_process("CDAT.workflow", max=float("inf"))
def workflow(self, context):
    """Holds the ouputs to a complex workflow.

    Each input represents a unique output. Domain and parameters defined in
    the workflow process will act like defaults for each child process.
    """
    context.started()

    interm = {}

    with metrics.TASK_BUILD_WORKFLOW_DURATION.time():
        for process in context.sorted:
            inputs = [
                x if isinstance(x, cwt.Variable) else interm[x.name]
                for x in process.inputs
            ]

            process_func = base.get_process(process.identifier)

            params = process_func._get_parameters(process)

            output = process_func._process_func(
                context, process, *inputs, **params
            )

            interm[process.name] = output

    context.message("Preparing to execute workflow")

    if "DASK_SCHEDULER" in context.extra:
        client = utilities.retry(8, 1)(Client)(
            context.extra["DASK_SCHEDULER"]
        )
    else:
        client = Client()

    try:
        delayed = context.delayed

        delayed.extend(
            gather_workflow_outputs(context, interm, context.output_ops())
        )

        context.message(f"Gathered {len(delayed)} outputs")

        with metrics.TASK_EXECUTE_WORKFLOW_DURATION.time():
            fut = client.compute(delayed)

            DaskTaskTracker(context, fut)
    except DaskTimeoutError:
        metrics.TASK_WORKFLOW_FAILURE.inc()

        client.cancel(delayed)

        raise
    except WPSError:
        metrics.TASK_WORKFLOW_FAILURE.inc()

        raise
    except Exception as e:
        metrics.TASK_WORKFLOW_FAILURE.inc()

        raise WPSError("Error executing process: {!r}", e)
    else:
        metrics.TASK_WORKFLOW_SUCCESS.inc()
    finally:
        client.close()

    metrics.push(context.job)

    context.succeeded(json.dumps([x.to_dict() for x in context.output]))

    return context


@bind_process_func(partial(process_dataset, func=lambda x: np.abs(x)))
@param_defaults()
@base.register_process("CDAT.abs")
def task_abs(self, context):
    """Computes elementwise absolute on each variable.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(partial(process_dataset_or_const, func=lambda x, y: x + y))
@param_const
@param_defaults()
@base.register_process("CDAT.add", max=2)
def task_add(self, context):
    """Computes an elementwise sum for each variable.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(process_aggregate)
@param_defaults("variable")
@base.register_process("CDAT.aggregate", min=2, max=float("inf"))
def task_aggregate(self, context):
    """Aggregates a variable spanning multiple input files."""
    return workflow(context)


@bind_process_func(partial(process_dataset_or_const, func=lambda x, y: x / y))
@param_const
@param_defaults()
@base.register_process("CDAT.divide", max=2)
def task_divide(self, context):
    """Compute elementwise division between a variable or constant.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(partial(process_dataset, func=lambda x: np.exp(x)))
@param_defaults()
@base.register_process("CDAT.exp")
def task_exp(self, context):
    """Computes elementwise exponent on each variable.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(process_filter_map)
@base.parameter(
    "func",
    "A reduction process to apply e.g. max, min, sum, mean.",
    str,
    min=1,
)
@param_other
@param_cond
@param_defaults()
@base.register_process("CDAT.filter_map")
def task_filter_map(self, context):
    """Applies a filter and function to a variable.

    See `CDAT.where` abstract for details on supported conditions.
    """
    return workflow(context)


@bind_process_func(partial(process_dataset, func=lambda x: np.log(x)))
@param_defaults()
@base.register_process("CDAT.log")
def task_log(self, context):
    """Computes the elementwise log for each variable.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(
    partial(process_reduce, func=lambda x, y: getattr(x, "max")(dim=y))
)
@param_axes
@param_defaults()
@base.register_process("CDAT.max")
def task_max(self, context):
    """Computes the maximum for each variable over one or more axes.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(
    partial(process_reduce, func=lambda x, y: getattr(x, "mean")(dim=y))
)
@param_axes
@param_defaults()
@base.register_process("CDAT.mean")
def task_mean(self, context):
    """Computes the mean for each variable over one or more axes.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(
    partial(process_reduce, func=lambda x, y: getattr(x, "min")(dim=y))
)
@param_axes
@param_defaults()
@base.register_process("CDAT.min")
def task_min(self, context):
    """Computes the minimum for each variable over one or more axes.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(partial(process_dataset_or_const, func=lambda x, y: x * y))
@param_const
@param_defaults()
@base.register_process("CDAT.multiply", max=2)
def task_multiply(self, context):
    """Computes the elementwise multi for each variable.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(
    partial(process_dataset_or_const, func=lambda x, y: x ** y)
)
@param_const
@param_defaults()
@base.register_process("CDAT.power")
def task_power(self, context):
    """Computes the elementwise power for each variable.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(process_subset)
@base.parameter("method", "method to apply", str)
@param_defaults()
@base.register_process("CDAT.subset")
def task_subset(self, context):
    """Computes the subset of each variable.

    When subsetting by values selection can be controlled with the
    `method` parameter.

    Possible values: pad, backfill, nearest
    """
    return workflow(context)


@bind_process_func(partial(process_dataset_or_const, func=lambda x, y: x - y))
@param_const
@param_defaults()
@base.register_process("CDAT.subtract", max=2)
def task_subtract(self, context):
    """Computes the difference between inputs or a constant.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(
    partial(process_reduce, func=lambda x, y: getattr(x, "sum")(dim=y))
)
@param_axes
@param_defaults()
@base.register_process("CDAT.sum")
def task_sum(self, context):
    """Computes the sum for each variable over one or more axes.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(process_merge)
@base.parameter(
    "compat",
    (
        "Method used to resolve conflicts, defaults to no_conflicts, can be"
        "changed to override to bypass checks."
    ),
    str,
)
@base.register_process("CDAT.merge", min=2, max=float("inf"))
def task_merge(self, context):
    """Merge variables from multiple inputs.

    Use parameter `compat` to control how conflicts are handled.
    """
    return workflow(context)


@bind_process_func(process_where)
@param_cond
@param_other
@param_defaults()
@base.register_process("CDAT.where")
def task_where(self, context):
    """Filters values based on condition.

    Supported comparisons: >, >=, <, <=, ==, !=

    Left hand side should be a variable or axis name and the right can be an
    int or float.

    Examples:
        lon>180
        pr>0.000023408767
        pr>=prw
    """
    return workflow(context)


@bind_process_func(process_groupby_bins)
@base.parameter(
    "bins",
    (
        "A list of bins boundaries. e.g. 0, 10, 20 would create 2 bins (0-10),"
        "(10,20)."
    ),
    list,
    float,
    min=1,
    max=float("inf"),
)
@param_defaults("fillna", "rename")
@base.register_process("CDAT.groupby_bins")
def task_groupby_bins(self, context):
    """Groups values of a variable into bins.

    This process is not lazily evaluated and it's processing time will scale
    with the size of input data.

    If the input to this process contains multiple variables use `variable`
    to specify which one to use.

    A variable can be grouped based of the values of second variables values,
    to accomplish this both variables must be present in the input, this can
    be accomplished by having the variables in the source or by calling
    `CDAT.merge` with multiple inputs.
    """
    return workflow(context)


@bind_process_func(
    partial(process_dataset, func=lambda x: getattr(x, "count")())
)
@param_defaults()
@base.register_process("CDAT.count")
def task_count(self, context):
    """Computes count on each variable.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(
    partial(process_dataset, func=lambda x: getattr(x, "squeeze")(drop=True))
)
@param_defaults()
@base.register_process("CDAT.squeeze")
def task_squeeze(self, context):
    """Squeezes each variable.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(
    partial(process_reduce, func=lambda x, y: getattr(x, "std")(dim=y))
)
@param_axes
@param_defaults()
@base.register_process("CDAT.std")
def task_std(self, context):
    """Computes the standard deviation on each variable over one or more axes.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(
    partial(process_reduce, func=lambda x, y: getattr(x, "var")(dim=y))
)
@param_axes
@param_defaults()
@base.register_process("CDAT.var")
def task_var(self, context):
    """Compute the variance on each variable over one or more axes.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)


@bind_process_func(partial(process_dataset, func=lambda x: np.sqrt(x)))
@param_defaults()
@base.register_process("CDAT.sqrt")
def task_sqrt(self, context):
    """Compute the elementwise square root on each variable.

    Can be applied to a single variable if the parameter `variable` is used.
    """
    return workflow(context)
