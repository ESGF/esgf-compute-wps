from wps import logger

from esgf import utils
from esgf import Domain
from esgf import WPSServerError

import cdms2
import genutil
from cdms2 import MV2

import numpy

import re

class DataContainer(object):
    COMP_MAP = {
        '<=': MV2.less_equal,
        '>=': MV2.greater_equal,
        '<': MV2.less,
        '>': MV2.greater,
    }

    def __init__(self, variable, domain=None, gridder=None):
        self._var_data = variable
        self._mask_data = None
        self._domain = domain
        self._gridder = gridder
        self._preprocessed = False

    @property
    def chunk(self):
        """ Retrieves chunks. """
        if not self._preprocessed:
            self._preprocess()

        return self._var_data

    def _preprocess(self):
        """ Applies preprocessing to the variable. """
        if self._domain and self._domain[0].mask:
            self._apply_mask(self._domain[0].mask)

        if self._gridder:
            self._apply_regrid()

    def _parse_operation(self, operation):
        """ Parses the arguments of an operation. """
        arg_pat = 'mask_data|var_data|[0-9]*\.?[0-9]*'

        op_pat = '|'.join(self.COMP_MAP.keys())

        full_pat = '(%s)(%s)(%s)' % (arg_pat, op_pat, arg_pat)
        
        match = re.match(full_pat, operation)

        if not match:
            raise WPSServerError('Masking %s failed, check operation for correctness. Supported arguments mask_data, var_data, decimal value. Supported operations %s' % (
                self._var.name,
                ','.join(self.COMP_MAP.keys())))

        return match.groups()

    def _resolve_arg(self, arg):
        """ Resolve an operation argument. """
        value = None

        if arg == 'var_data':
            value = self._var_data
        elif arg == 'mask_data':
            if not self._mask_data:
                mask_file = cdms2.open(self._domain[0].mask.uri, 'r')

                self._mask_data = mask_file[self._domain[0].mask.var_name]

            value = self._mask_data
        else:
            value = utils.int_or_float(arg)

        return value

    def _apply_mask(self, mask):
        """ Apply a mask to a variable. """
        left, ineq_sym, right = self._parse_operation(mask.operation)
        
        left = self._resolve_arg(left)

        right = self._resolve_arg(right)
    
        ineq = self.COMP_MAP[ineq_sym] 
    
        if (isinstance(left, cdms2.fvariable.FileVariable) and
                left.shape != self._var_data.shape):
            left, self._var_data = genutil.grower(left, self._var_data)
        elif (isinstance(right, cdms2.fvariable.FileVariable) and
              right.shape != self._var_data.shape):
            right, self._var_data = genutil.grower(right, self._var_data) 

        self._var_data = MV2.masked_where(ineq(left, right), self._var_data)

    def _generate_grid_from_domain(self, domain):
        """ Generate a target grid from a Domain definition. """
        try:
            lat = [x for x in domain.dimensions if x.name == 'latitude'][0]

            lon = [x for x in domain.dimensions if x.name == 'longitude'][0]
        except IndexError:
            raise WPSServerError('Target grid must have latitude and longitude dimensions.')

        lat_n = abs(lat.end-lat.start)/lat.step

        lon_n = abs(lon.end-lon.start)/lon.step

        return cdms2.createUniformGrid(lat.start, lat_n, lat.step, lon.start, lon_n, lon.step)

    def _apply_regrid(self):
        """ Applys a regrid to a variable. """
        if not isinstance(self._gridder.grid, Domain):
            raise WPSServerError('Target grid %t not supported' % (self._gridder.grid,))

        target_grid = self._generate_grid_from_domain(self._gridder.grid)

        self._var_data = self._var_data.regrid(target_grid)
