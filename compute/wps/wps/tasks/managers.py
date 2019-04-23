import re
import logging
import urllib
from collections import OrderedDict
from past.utils import old_div

import cdms2
import cwt
import dask
import dask.array as da
import requests
import xarray as xr
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError

from wps import metrics
from wps import WPSError
from wps.tasks import credentials
from wps.tasks.dask_serialize import regrid_chunk
from wps.tasks.dask_serialize import retrieve_chunk

logger = logging.getLogger('wps.tasks.manager')


class InputManager(object):
    def __init__(self, fm, uris, var_name):
        self.fm = fm

        self.uris = uris

        self.var_name = var_name

        self.domain = None

        self.map = OrderedDict()

        self.data = None

        self.attrs = {}

        self.vars = {}

        self.vars_axes = {}

        self.axes = {}

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

    def copy(self):
        new = InputManager(self.fm, self.uris, self.var_name)

        new.domain = self.domain

        new.map = self.map.copy()

        new.data = self.data

        new.attrs = self.attrs.copy()

        for x, y in self.vars.items():
            if x == self.var_name:
                new.vars[x] = y
            else:
                new.vars[x] = y.clone()

        new.vars_axes = self.vars_axes.copy()

        new.axes = dict((x, y.clone()) for x, y in self.axes.items())

        return new

    def subset_grid(self, grid, selector):
        target = cdms2.MV2.ones(grid.shape)

        logger.debug('Target grid %r'. target.shape)

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
        url_validator = URLValidator(['https', 'http'])

        try:
            url_validator(gridder.grid.uri)
        except ValidationError:
            raise WPSError('Path to grid file is not an OpenDAP url: {}', gridder.grid.uri)

        try:
            with cdms2.open(gridder.grid) as infile:
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

    def regrid(self, gridder):
        if 'lat' in self.map and 'lon' in self.map:
            if len(self.vars) == 0:
                self.load_variables_and_axes()

            selector = dict((x, (self.axes[x][0], self.axes[x][-1])) for x in self.map.keys())

            grid, tool, method = self.regrid_context(gridder, selector)

            delayed = self.data.to_delayed().squeeze()

            if delayed.size == 1:
                delayed = delayed.reshape((1, ))

            # Create list of axis data that is present in the selector.
            axis_data = [self.axes[x] for x in selector.keys()]

            regrid_delayed = [dask.delayed(regrid_chunk)(x, axis_data, grid, tool, method) for x in delayed]

            regrid_arrays = [da.from_delayed(x, y.shape[:-2] + grid.shape, self.data.dtype)
                             for x, y in zip(regrid_delayed, self.data.blocks)]

            self.data = self.vars[self.var_name] = da.concatenate(regrid_arrays)

            logger.info('Concatenated regridded arrays %r', self.data)

            self.axes['lat'] = grid.getLatitude()

            self.vars['lat_bnds'] = self.axes['lat'].getBounds()

            self.axes['lon'] = grid.getLongitude()

            self.vars['lon_bnds'] = self.axes['lon'].getBounds()

    def process(self, axes, process):
        map_keys = list(self.map.keys())

        indices = tuple(map_keys.index(x) for x in axes)

        logger.info('Mapped axes %r to indices %r', axes, indices)

        self.data = process(self.data, axis=indices)

        logger.info('Processed data %r', self.data)

        for axis in axes:
            logger.info('Removing axis %r', axis)

            self.map.pop(axis)

    def new_shape(self, shape, time_slice):
        diff = time_slice.stop - time_slice.start

        return (diff, ) + shape[1:]

    def from_delayed(self, uri, var, chunk_shape):
        size = var.shape[0]

        step = chunk_shape[0]

        chunks = [slice(x, min(size, x+step), 1) for x in range(0, size)]

        delayed = [da.delayed(retrieve_chunk)(uri, var.id, {'time': x}, self.fm.user.auth.cert) for x in chunks]

        arrays = [da.from_delayed(x, self.new_shape(var.shape, y), var.dtype) for x, y in zip(delayed, chunks)]

        return da.concatenate(arrays, axis=0)

    def slice_to_subaxis(self, axis):
        axis_slice = self.map.get(axis.id, slice(None, None, None))

        i = axis_slice.start or 0
        j = axis_slice.stop or len(axis)
        k = axis_slice.step or 1

        return i, j, k

    def load_variables_and_axes(self):
        time_axis = []
        time_bnds = []

        for uri in self.uris:
            file = self.fm.open_file(uri)

            if 'global' not in self.attrs:
                self.attrs['global'] = file.attributes.copy()

            for var in file.getVariables():
                skip_var = False

                for axis in var.getAxisList():
                    if var.id != self.var_name and axis.id not in self.map and axis.id != 'nbnd':
                        skip_var = True

                        break

                    if axis.id not in self.map and axis.id != 'nbnd':
                        continue

                    if var.id in self.vars_axes:
                        self.vars_axes[var.id].append(axis.id)
                    else:
                        self.vars_axes[var.id] = [axis.id, ]

                    if axis.id not in self.attrs:
                        self.attrs[axis.id] = axis.attributes.copy()

                    if axis.isTime():
                        time_axis.append(axis.clone())
                    else:
                        i, j, k = self.slice_to_subaxis(axis)

                        self.axes[axis.id] = axis.subAxis(i, j, k)

                if skip_var:
                    continue

                if var.getTime() is not None and var.id != self.var_name:
                    time_bnds.append(var())
                elif var.id not in self.vars:
                    if var.id == self.var_name:
                        self.vars[var.id] = self.data
                    else:
                        selector = dict((x.id, self.map[x.id]) for x in var.getAxisList()
                                        if x in self.map or x == 'nbnd')

                        self.vars[var.id] = var(**selector)

                if var.id not in self.attrs:
                    self.attrs[var.id] = var.attributes.copy()

        if len(time_axis) > 0:
            axis_concat = cdms2.MV.axisConcatenate(time_axis, id='time')

            i, j, k = self.slice_to_subaxis(axis_concat)

            self.axes['time'] = axis_concat.subAxis(i, j, k)

        if len(time_bnds) > 0:
            var_concat = cdms2.MV.concatenate(time_bnds)

            selector = dict((x.id, self.map[x.id]) for x in time_bnds[0].getAxisList() if x.id in self.map)

            self.vars['time_bnds'] = var_concat(**selector)

    def to_xarray(self):
        axes = {}
        vars = {}

        if len(self.vars) == 0:
            self.load_variables_and_axes()

        for x, y in self.axes.items():
            axes[x] = xr.DataArray(y, name=x, dims=x, attrs=self.attrs[x])

        for x, y in self.vars.items():
            coords = dict((z, axes[z]) for z in self.vars_axes[x])

            vars[x] = xr.DataArray(y, name=x, dims=coords.keys(), coords=coords, attrs=self.attrs[x])

        return xr.Dataset(vars, attrs=self.attrs['global'])

    def subset(self, domain):
        data = []

        logger.info('Subsetting the inputs')

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

        self.map_domain(domain)

        selector = tuple(self.map.values())

        self.data = data[selector]

        logger.info('Subsetted data %r', self.data)

    def map_dimension(self, dim, axis):
        if not isinstance(dim, slice):
            try:
                interval = axis.mapInterval(dim[:2])
            except TypeError:
                dim = None
            else:
                if len(dim) > 2:
                    step = dim[2]
                else:
                    step = 1

                dim = slice(interval[0], interval[1], step)

        return dim

    def sort_uris(self, ordering):
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

        return [x[0] for x in ordering]

    def adjust_time_axis(self, time_maps):
        start = None
        stop = None
        step = None

        logger.info('Adjusting time axis')

        for uri in self.uris:
            time_slice = time_maps[uri]

            if start is None:
                start = time_slice.start

                stop = time_slice.stop

                step = time_slice.step

                logger.info('Setting initial values start %r stop %r step %r', start, stop, step)
            elif time_slice is None:
                logger.info('Skipping file %r', uri)

                break
            else:
                before = stop

                stop += (time_slice.stop - time_slice.start)

                logger.info('Extending time axis %r -> %r', before, stop)

        return slice(start, stop, step)

    def map_domain(self, domain):
        self.domain = self.domain_to_dict(domain)

        time_maps = {}
        ordering = []

        logger.info('Mapping domain')

        for uri in self.uris:
            logger.info('Processing input %r', uri)

            var = self.fm.get_variable(uri, self.var_name)

            for axis in var.getAxisList():
                if axis.isTime():
                    ordering.append((axis.units, axis[0]))

                    logger.info('Added ordering %r', ordering[-1])

                if axis.id in self.map and not axis.isTime():
                    continue

                try:
                    dim = self.domain[axis.id]
                except KeyError:
                    self.map[axis.id] = slice(None, None, None)
                else:
                    self.map[axis.id] = self.map_dimension(dim, axis)

                    if axis.isTime():
                        time_maps[uri] = self.map[axis.id]

        if len(self.uris) > 1:
            ordering = [(x, ) + y for x, y in zip(self.uris, ordering)]

            self.uris = self.sort_uris(ordering)

            dim_time = domain.get_dimension('time')

            if 'time' in self.domain and dim_time.crs == cwt.INDICES:
                self.map['time'] = self.adjust_time_axis(time_maps)

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
    def __init__(self, user):
        self.user = user

        self.handles = {}

        self.auth = {}

    def requires_cert(self, uri):
        return uri in self.auth

    def open_file(self, uri):
        logger.info('Opening file %r', uri)

        if uri not in self.handles:
            logger.info('File has not been open before')

            if not self.check_access(uri):
                if not self.check_access(uri, self.user.auth.cert):
                    raise WPSError('File {!r} is not accessible, check the OpenDAP service', uri)

                credentials.load_certificate(self.user)

                logger.info('File %r requires certificate', uri)

                self.auth[uri] = True

            try:
                self.handles[uri] = cdms2.open(uri)
            except cdms2.CDMSError:
                raise WPSError('CDMS failed to open {!r}', uri)

        return self.handles[uri]

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
            response = requests.get(url, timeout=(2, 30), cert=cert, verify=False)
        except requests.ConnectTimeout:
            logger.exception('Timeout connecting to %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise WPSError('Timeout connecting to {!r}', parts.hostname)
        except requests.ReadTimeout:
            logger.exception('Timeout reading from %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise WPSError('Timeout reading from {!r}', parts.hostname)

        if response.status_code == 200:
            return True

        logger.info('Checking url failed with status code %r', response.status_code)

        metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

        return False
