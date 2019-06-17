import os
import re
import logging
import urllib
import tempfile
from collections import OrderedDict
from past.utils import old_div

import cdms2
import cwt
import dask
import dask.array as da
import requests
import xarray as xr

from compute_tasks import metrics_ as metrics
from compute_tasks import AccessError
from compute_tasks import WPSError
from compute_tasks.dask_serialize import retrieve_chunk

logger = logging.getLogger('compute_tasks.manager')


BOUND_NAMES = ('nbnd', 'bnds')


class InputManager(object):
    def __init__(self, fm, uris, var_name):
        self.fm = fm

        self.uris = uris

        self.var_name = var_name

        self.attrs = {}

        self.vars = {}

        self.vars_axes = {}

        self.axes = {}

    def remove_axis(self, axis_name):
        try:
            del self.axes[axis_name]
        except KeyError as e:
            raise WPSError('Did not find axis {!r}', e)
        else:
            for name in list(self.vars_axes.keys()):
                if name == self.var_name:
                    try:
                        index = self.vars_axes[name].index(axis_name)
                    except ValueError:
                        pass
                    else:
                        self.vars_axes[name].pop(index)
                else:
                    if axis_name in self.vars_axes[name]:
                        try:
                            del self.vars_axes[name]

                            del self.vars[name]
                        except KeyError:
                            pass

    def __repr__(self):
        return ('InputManager(uris={!r}, var_name={!r}, attrs={!r}, '
                'vars={!r}, vars_axes={!r}, axes={!r}').format(self.uris, self.var_name,
                                                               self.attrs, self.vars,
                                                               self.vars_axes, self.axes)

    @classmethod
    def from_cwt_variable(cls, fm, variable):
        logger.info('Creating manager with single file')

        return cls(fm, [variable.uri, ], variable.var_name)

    @classmethod
    def from_cwt_variables(cls, fm, variables):
        var_names = [x.var_name for x in variables]

        if len(set(var_names)) > 1:
            raise WPSError('Mismatched variable names {!r}', var_names)

        uris = [x.uri for x in variables]

        logger.info('Creating manager with %r files', len(uris))

        return cls(fm, uris, var_names[0])

    @property
    def shape(self):
        return self.variable.shape

    @property
    def nbytes(self):
        return self.variable.nbytes

    @property
    def dtype(self):
        return self.variable.dtype

    @property
    def blocks(self):
        return self.variable.blocks

    @property
    def variable(self):
        return self.vars[self.var_name]

    @variable.setter
    def variable(self, value):
        self.vars[self.var_name] = value

    @property
    def variable_axes(self):
        return self.vars_axes[self.var_name]

    def copy(self):
        new = InputManager(self.fm, self.uris, self.var_name)

        new.attrs = self.attrs.copy()

        for x, y in self.vars.items():
            if x == self.var_name:
                new.vars[x] = y
            else:
                new.vars[x] = y.copy()

        new.vars_axes = self.vars_axes.copy()

        new.axes = dict((x, y.clone()) for x, y in self.axes.items())

        return new

    def subset_grid(self, grid, selector):
        target = cdms2.MV2.ones(grid.shape)

        logger.debug('Target grid %r', target.shape)

        target.setAxisList(grid.getAxisList())

        logger.info('Subsetting grid with selector %r', selector)

        target = target(**selector)

        logger.debug('Target grid new shape %r', target.shape)

        return target.getGrid()

    def parse_uniform_arg(self, value, default_start, default_n):
        result = re.match('^(\\d\\.?\\d?)$|^(-?\\d\\.?\\d?):(\\d\\.?\\d?):(\\d\\.?\\d?)$', value)

        if result is None:
            raise WPSError('Failed to parse uniform argument {value}', value=value)

        groups = result.groups()

        if groups[1] is None:
            delta = int(groups[0])

            default_n = old_div(default_n, delta)
        else:
            default_start = int(groups[1])

            default_n = int(groups[2])

            delta = int(groups[3])

        start = default_start + (delta / 2.0)

        logger.info('Parsed uniform args start %r default_n %r delta %r', start, default_n, delta)

        return start, default_n, delta

    def generate_user_defined_grid(self, gridder):
        try:
            grid_type, grid_param = gridder.grid.split('~')
        except AttributeError:
            return None
        except ValueError:
            raise WPSError('Error generating grid "{name}"', name=gridder.grid)

        logger.info('Generating grid %r %r', grid_type, grid_param)

        if grid_type.lower() == 'uniform':
            result = re.match('^(.*)x(.*)$', grid_param)

            if result is None:
                raise WPSError('Failed to parse uniform configuration from {value}', value=grid_param)

            try:
                start_lat, nlat, delta_lat = self.parse_uniform_arg(result.group(1), -90.0, 180.0)
            except WPSError:
                raise

            try:
                start_lon, nlon, delta_lon = self.parse_uniform_arg(result.group(2), 0.0, 360.0)
            except WPSError:
                raise

            grid = cdms2.createUniformGrid(start_lat, nlat, delta_lat, start_lon, nlon, delta_lon)

            logger.info('Created target uniform grid {} from lat {}:{}:{} lon {}:{}:{}'.format(
                grid.shape, start_lat, delta_lat, nlat, start_lon, delta_lon, nlon))
        elif grid_type.lower() == 'gaussian':
            try:
                nlats = int(grid_param)
            except ValueError:
                raise WPSError('Error converting gaussian parameter to an int')

            grid = cdms2.createGaussianGrid(nlats)

            logger.info('Created target gaussian grid {}'.format(grid.shape))
        else:
            raise WPSError('Unknown grid type for regridding: {}', grid_type)

        return grid

    def read_grid_from_file(gridder):
        try:
            with cdms2.open(gridder.grid.uri) as infile:
                data = infile(gridder.grid.var_name)
        except cdms2.CDMSError:
            raise WPSError('Failed to read the grid from {} in {}', gridder.grid.var_name, gridder.grid.uri)

        return data.getGrid()

    def generate_grid(self, gridder, selector):
        try:
            if isinstance(gridder.grid, cwt.Variable):
                grid = self.read_grid_from_file(gridder)
            else:
                grid = self.generate_user_defined_grid(gridder)
        except AttributeError:
            # Handle when gridder is None
            return None

        grid = self.subset_grid(grid, selector)

        return grid

    def regrid_context(self, gridder, selector):
        grid = self.generate_grid(gridder, selector)

        if isinstance(gridder.grid, cwt.Variable):
            grid_src = gridder.grid.uri
        else:
            grid_src = gridder.grid

        metrics.WPS_REGRID.labels(gridder.tool, gridder.method, grid_src).inc()

        return grid, gridder.tool, gridder.method

    def new_shape(self, shape, time_slice):
        diff = time_slice.stop - time_slice.start

        return (diff, ) + shape[1:]

    def from_delayed(self, uri, var, chunk_shape):
        size = var.shape[0]

        step = chunk_shape[0]

        logger.info('Building input from delayed functions size %r step %r', size, step)

        chunks = [slice(x, min(size, x+step), 1) for x in range(0, size, step)]

        logger.info('Generated %r chunks', len(chunks))

        delayed = [dask.delayed(retrieve_chunk)(uri, var.id, {'time': x}, self.fm.cert_data) for x in chunks]

        arrays = [da.from_delayed(x, self.new_shape(var.shape, y), var.dtype) for x, y in zip(delayed, chunks)]

        return da.concatenate(arrays, axis=0)

    def slice_to_subaxis(self, axis_slice, axis):
        i = axis_slice.start or 0
        j = axis_slice.stop or len(axis)
        k = axis_slice.step or 1

        return i, j, k

    def load_variables_and_axes(self, target_variable):
        attrs = {}
        axes = {}
        vars = {}
        vars_axes = {}

        time_axis = []
        units = None

        for uri in self.uris:
            file = self.fm.open_file(uri)

            if 'global' not in attrs:
                logger.info('Storing global attributes')

                attrs['global'] = file.attributes.copy()

            stored_time = False

            for var in file.getVariables():
                # Skip all variables already discovered and do not contain temporal axis
                if var.id in vars and var.getTime() is None:
                    logger.info('Skipping variable %r, already discovered')

                    continue

                axis_ids = []

                for axis in var.getAxisList():
                    if axis.isTime():
                        if units is None:
                            units = axis.units

                            logger.info('Setting base units to %r', units)

                        if not stored_time:
                            time_axis_clone = axis.clone()

                            time_axis_clone.toRelativeTime(units)

                            time_axis.append(time_axis_clone)

                            stored_time = True

                            attrs[axis.id] = axis.attributes.copy()

                            logger.info('Storing temporal axis %r shape %r', axis.id, axis.shape)
                    elif axis.id not in axes:
                        axes[axis.id] = axis.clone()

                        attrs[axis.id] = axis.attributes.copy()

                        logger.info('Storing spatial axis %r shape %r', axis.id, axis.shape)
                    else:
                        logger.info('Axis %r is not time and already discovered', axis.id)

                    axis_ids.append(axis.id)

                vars_axes[var.id] = axis_ids

                logger.info('Setting variable %r axes to %r', var.id, axis_ids)

                attrs[var.id] = var.attributes.copy()

                if var.id == self.var_name:
                    var_data = target_variable
                else:
                    var_data = var()

                vars[var.id] = var_data

                logger.info('Storing variable %r shape %r', var.id, var.shape)

        if len(time_axis) > 1:
            axes['time'] = cdms2.MV2.axisConcatenate(time_axis, id='time', attributes=attrs['time'])

            logger.info('Updating temporal axis shape %r', axes['time'].shape)

            vars['time_bnds'] = cdms2.createVariable(
                axes['time'].getBounds(),
                axes=[axes[x] for x in vars_axes['time_bnds']],
                id='time_bnds',
                attributes=attrs['time_bnds'],
            )

            logger.info('Updating temporal bounds shape %r', vars['time_bnds'].shape)
        else:
            axes['time'] = time_axis[0]

        return attrs, vars, axes, vars_axes

    def subset_variables_and_axes(self, domain, vars, axes, vars_axes):
        map = self.map_domain(domain, axes)

        for name in list(axes.keys()):
            if name in BOUND_NAMES:
                continue

            axis_slice = map.get(name, slice(None, None, None))

            try:
                i, j, k = self.slice_to_subaxis(axis_slice, axes[name])
            except AttributeError:
                raise WPSError('Failed to convert slice to subaxis for axis {!r}', name)

            shape = axes[name].shape

            axes[name] = axes[name].subAxis(i, j, k)

            logger.info('Subsetting axis %r shape %r -> %r', name, shape, axes[name].shape)

        for name in list(vars.keys()):
            if name in self.vars_axes:
                selector = OrderedDict((x, map[x]) for x in vars_axes[name] if x in map)
            else:
                selector = {}

            logger.info('Variable %r selector %r', name, selector)

            shape = vars[name].shape

            # Applying selector fails take the whole axis
            try:
                if name == self.var_name:
                    dask_selector = tuple(selector.values())

                    logger.info('Dask selector %r', dask_selector)

                    vars[name] = vars[name][dask_selector]
                else:
                    vars[name] = vars[name](**selector)
            except TypeError:
                vars[name] = vars[name]

            logger.info('Subsetting variable %r shape %r -> %r', name, shape, vars[name].shape)

        return vars, axes

    def to_xarray(self):
        axes = {}
        vars = {}

        logger.info('Building xarray with axes %r', self.axes.keys())
        logger.info('Building xarray with variables %r', self.vars.keys())

        for key, value in self.vars_axes.items():
            coords = OrderedDict()

            for axis_key in value:
                if axis_key in axes:
                    axis_value = axes[axis_key]
                else:
                    axis_value = axes[axis_key] = xr.DataArray(self.axes[axis_key], name=axis_key, dims=axis_key,
                                                               attrs=self.attrs[axis_key])

                    logger.info('Creating axis %r sizes %r', axis_value.name, axis_value.sizes)

                coords[axis_key] = axis_value

            vars[key] = xr.DataArray(self.vars[key], name=key, dims=coords.keys(), coords=coords, attrs=self.attrs[key])

            logger.info('Creating variable %r coords %r sizes %r', key, coords.keys(), vars[key].sizes)

        return xr.Dataset(vars, attrs=self.attrs['global'])

    def subset(self, domain):
        data = []

        logger.info('Subsetting the inputs')

        if len(self.uris) > 1:
            self.sort_uris()

        for uri in self.uris:
            var = self.fm.get_variable(uri, self.var_name)

            chunks = (100, ) + var.shape[1:]

            if self.fm.requires_cert(uri):
                data.append(self.from_delayed(uri, var, chunks))
            else:
                data.append(da.from_array(var, chunks=chunks))

            logger.info('Created input %r', data[-1])

        if len(data) > 1:
            data = da.concatenate(data, axis=0)

            logger.info('Concatenating data %r', data)
        else:
            data = data[0]

        attrs, vars, axes, vars_axes = self.load_variables_and_axes(data)

        self.attrs = attrs

        self.vars_axes = vars_axes

        vars, axes = self.subset_variables_and_axes(domain, vars, axes, vars_axes)

        self.vars = vars

        self.axes = axes

        return data, self.vars[self.var_name]

    def sort_uris(self):
        ordering = []

        for uri in self.uris:
            var = self.fm.get_variable(uri, self.var_name)

            time = var.getTime()

            if time is None:
                raise WPSError('Unable to sort inputs {!r} has no time axis', uri)

            ordering.append((uri, time.units, time[0]))

        logger.info('Sorting uris with %r', ordering)

        by_units = set([x[1] for x in ordering]) != 1

        by_first = set([x[2] for x in ordering]) != 1

        if by_units:
            ordering = sorted(ordering, key=lambda x: x[1])

            logger.info('Sorted uris by units')
        elif by_first:
            ordering = sorted(ordering, key=lambda x: x[2])

            logger.info('Sorted uris by first values')
        else:
            raise WPSError('Unable to determine ordering of files')

        self.uris = [x[0] for x in ordering]

        logger.info('Sorted uris %r', self.uris)

    def map_dimension(self, dim, axis):
        if not isinstance(dim, slice):
            try:
                interval = axis.mapInterval(dim[:2])
            except TypeError:
                new_dim = None
            else:
                if len(dim) > 2:
                    step = dim[2]
                else:
                    step = 1

                # Prevent wrapping by taking min of stop value and len of axis
                new_dim = slice(interval[0], min(interval[1], len(axis)), step)
        else:
            new_dim = dim

        logger.info('Mapped dimension %r shape %r -> %r', axis.id, dim, new_dim)

        return new_dim

    def map_domain(self, domain, axes):
        domain_dict = self.domain_to_dict(domain)

        logger.info('Mapping domain %r', domain_dict)

        map = OrderedDict()

        for name, value in axes.items():
            try:
                dim = domain_dict[name]
            except KeyError:
                map[name] = slice(None, None, None)
            else:
                map[name] = self.map_dimension(dim, value)

        return map

    def domain_to_dict(self, domain):
        """ Converts a domain to a dict.

        Args:
            domain: A cwt.Domain to convert.

        Returns:
            A dict mapping dimension names to the formatted dimension value.
        """
        if domain is None:
            return {}

        output = dict((x, self.format_dimension(y)) for x, y in domain.dimensions.items())

        logger.info('Converted domain to %r', output)

        return output

    def format_dimension(self, dim):
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

        logger.info('Formatted dimension as %r', data)

        return data


class FileManager(object):
    def __init__(self, context):
        self.context = context

        self.handles = {}

        self.auth = {}

        self.temp_dir = None

        self.old_cwd = None

        self.cert_path = None

        self.cert_data = None

    def __del__(self):
        # Revert to old cwd
        os.chdir(self.old_cwd)

        logger.info('Changed working directory %r to %r', self.temp_dir.name, self.old_cwd)

    def requires_cert(self, uri):
        return uri in self.auth

    def open_file(self, uri):
        logger.info('Opening file %r', uri)

        if uri not in self.handles:
            logger.info('File has not been open before')

            if not self.check_access(uri):
                cert_path = self.load_certificate()

                if not self.check_access(uri, cert_path):
                    raise WPSError('File {!r} is not accessible, check the OpenDAP service', uri)

                logger.info('File %r requires certificate', uri)

                self.auth[uri] = True

            try:
                self.handles[uri] = cdms2.open(uri)
            except Exception as e:
                raise AccessError(uri, e)

        return self.handles[uri]

    def load_certificate(self):
        """ Loads a user certificate.

        First the users certificate is checked and refreshed if needed. It's
        then written to disk and the processes current working directory is
        set, allowing calls to NetCDF library to use the certificate.

        Args:
            user: User object.
        """
        if self.cert_path is not None:
            return self.cert_path

        self.cert_data = self.context.user_cert()

        self.temp_dir = tempfile.TemporaryDirectory()

        logger.info('Using temporary directory %r', self.temp_dir.name)

        self.old_cwd = os.getcwd()

        os.chdir(self.temp_dir.name)

        logger.info('Changed working directory %r to %r', self.old_cwd, self.temp_dir.name)

        self.cert_path = os.path.join(self.temp_dir.name, 'cert.pem')

        with open(self.cert_path, 'w') as outfile:
            outfile.write(self.cert_data)

        logger.info('Wrote user certificate')

        dodsrc_path = os.path.join(self.temp_dir.name, '.dodsrc')

        with open(dodsrc_path, 'w') as outfile:
            outfile.write('HTTP.COOKIEJAR=.dods_cookies\n')
            outfile.write('HTTP.SSL.CERTIFICATE={}\n'.format(self.cert_path))
            outfile.write('HTTP.SSL.KEY={}\n'.format(self.cert_path))
            outfile.write('HTTP.SSL.VERIFY=0\n')

        logger.info('Wrote .dodsrc file {}'.format(dodsrc_path))

        return self.cert_path

    def get_variable(self, uri, var_name):
        file = self.open_file(uri)

        logger.info('Retieving FileVariable %r', var_name)

        var = file[var_name]

        if var is None:
            raise WPSError('Did not find variable {!r} in {!r}', var_name, uri)

        return var

    def check_access(self, uri, cert=None):
        url = '{!s}.dds'.format(uri)

        logger.info('Checking access with %r', url)

        parts = urllib.parse.urlparse(url)

        try:
            response = requests.get(url, timeout=(10, 30), cert=cert, verify=False)
        except requests.ConnectionError:
            logger.exception('Connection error %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise AccessError(url, 'Connection error to {!r}'.format(parts.hostname))
        except requests.ConnectTimeout:
            logger.exception('Timeout connecting to %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise AccessError(url, 'Timeout connecting to {!r}'.format(parts.hostname))
        except requests.ReadTimeout:
            logger.exception('Timeout reading from %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise AccessError(url, 'Timeout reading from {!r}'.format(parts.hostname))

        if response.status_code == 200:
            return True

        logger.info('Checking url failed with status code %r', response.status_code)

        metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

        return False
