"""
ESGFProcess module.
"""

from pywps import config
from pywps.Process import WPSProcess

from esgf import Domain
from esgf import Operation
from esgf import NamedParameter
from esgf import Parameter
from esgf import Variable
from esgf import Gridder
from esgf import WPSServerError
from esgf import utils

from uuid import uuid4 as uuid

from tempfile import NamedTemporaryFile

import os
import re
import sys
import json
import types
import mimetypes

import cdms2
import genutil
from cdms2 import MV2

from wps import logger
from wps.conf import settings

PYWPS_OUTPUT = config.getConfigValue('server', 'outputPath', '/var/lib/wps')

class ESGFProcess(WPSProcess):
    """ ESGF Process.

    Represents an ESGF WPS Process. Subclass this and override the __call__
    function. This function is where all the work should be done. From
    __calll__ return a list of output files. 
    """
    def __init__(self, title, **kwargs):

        """ ESGFProcess init. """
        WPSProcess.__init__(
            self,
            '.'.join(os.path.splitext(sys.modules[self.__module__].__file__)[0].split('/')[-2:]),
            title,
            abstract=kwargs.get('abstract', ''),
            version=kwargs.get('version', None),
            statusSupported=True,
            storeSupported=True)

        self.addComplexInput(
            'domain',
            'Domain',
            'Domain the process will utilize.',
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[
                {'mimeType': 'text/json'}
            ],
            maxmegabites=None)

        self.addComplexInput(
            'variable',
            'Variable',
            'Variable the process will execute on.',
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[
                {'mimeType': 'text/json'}
            ],
            maxmegabites=None)

        self.addComplexInput(
            'operation',
            'Operation',
            'Operation/Arguments for the process.',
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            formats=[
                {'mimeType': 'text/json'}
            ],
            maxmegabites=None)

        self.addComplexOutput(
            'output',
            'Output',
            'Process output.',
            metadata=[],
            formats=[
                {'mimeType': 'text/json'},
            ],
            useMapscript=False,
            asReference=False)

        self._variable = []
        self._operation = []
        self._output = None

        self._symbols = {}

    def _read_input_literal(self, identifier):
        """ Reads a literal input value. """
        return self.getInputValue(identifier) 

    def _read_input(self, identifier):
        """ Reads a complex type from a temporary file. """
        temp_file_path = self.getInputValue(identifier)

        with open(temp_file_path, 'r') as temp_file:
            return temp_file.readlines()

    def _load_operation(self):
        """ Loads the processes operation. """
        op_param = self._read_input('operation')[0]

        logger.info('Received operations %s', op_param)

        op_dict = json.loads(op_param)

        for op in op_dict:
            op_obj = Operation.from_dict(op)

            logger.info('Loaded operation %r', op_obj)

            self._operation.append(op_obj)

    def _load_variable(self):
        """ Loads the variable to be processed. """
        var_param = self._read_input('variable')[0]

        logger.info('Received variables %s', var_param)

        var_dict = json.loads(var_param)

        for var in var_dict:
            var_obj = Variable.from_dict(var, self._symbols)

            logger.info('Loaded variable %r', var_obj)

            self._variable.append(var_obj)

    def _load_domains(self):
        """ Loads the domains that will be used in the process. """
        dom_param = self._read_input('domain')[0]

        logger.info('Received domains %s', dom_param)

        dom_dict = json.loads(dom_param)

        for dom in dom_dict:
            dom_obj = Domain.from_dict(dom)

            logger.info('Loaded domain %r', dom_obj)

            self._symbols[dom_obj.name] = dom_obj

    def _load_data(self):
        """ Loads all the required data for the process. """
        self._load_domains()

        self._load_variable()

        self._load_operation()

        gridder = None

        for op in self._operation:
            for param in op.parameters:
                if isinstance(param, NamedParameter):
                    self._symbols[param.name] = param.values
                elif isinstance(param, Gridder):
                    gridder = param
        
        for var in self._variable:
            file_obj = cdms2.open(var.uri, 'r')

            var_obj = file_obj(var.var_name)

            if var.domains and var.domains[0].mask:
                var_obj = self._apply_mask(var.domains[0].mask, var_obj)

            self._symbols[var.name] = var_obj

            if gridder:
                try:
                    var_regrid = self._regrid(gridder, self._symbols[var.name])
                except Exception as e:
                    raise WPSServerError('Regridding failed: %s' % (e.message,))
                else:
                    self._symbols[var.name] = var_regrid

    def _apply_mask(self, mask, var_obj):
        """ Applys mask to variable. """
        logger.info('Applying mask %r', mask)

        mask_file = cdms2.open(mask.uri)

        mask_var = mask_file(mask.var_name)

        value_pattern = 'mask_data|var_data|[0-9]*\.?[0-9]*'
        equality_pattern = '<=|>=|<|>'
        full_pattern = '(%s)(%s)(%s)' % (value_pattern,
                                         equality_pattern,
                                         value_pattern)

        res = re.match(full_pattern, mask.operation)

        if not res:
            raise WPSServerError('Invalid mask operation: %s' % (mask.operation,))

        op_map = {
            '<=': MV2.less_equal,
            '>=': MV2.greater_equal,
            '<': MV2.less,
            '>': MV2.greater,
        }

        left, op, right = res.groups()

        left_data = self._get_data(var_obj, mask_var, left)
        right_data = self._get_data(var_obj, mask_var, right)

        mask_data = op_map[op](left_data, right_data)

        if mask_data.shape != var_obj.shape:
            mask_data, var_obj = genutil.grower(mask_data, var_obj)

        mask_var = MV2.masked_where(mask_data, var_obj)

        return mask_var

    def _get_data(self, var_obj, mask_var, data):
        """ Return literal value or referenced data. """
        data_map = {
            'var_data': var_obj,
            'mask_data': mask_var,
        }

        try:
            return data_map[data]
        except KeyError:
            # Return a literal value
            return utils.int_or_float(data)

    def _regrid(self, gridder, variable):
        """ Attempts to regrid a variable. """
        try:
            grid = self._symbols[gridder.grid]
        except KeyError:
            raise WPSServerError('Unable to generate grid %s', gridder.grid)

        if isinstance(grid, Variable):
            temp_file = cdms2.open(grid.uri, 'r')

            new_grid = temp_file[grid.var_name].getGrid()
        else:
            new_grid = self._new_grid_from_domain(grid)

        return variable.regrid(new_grid, grid_tool=gridder.tool, grid_method=gridder.method)

    def _new_grid_from_domain(self, domain):
        """ Creates a new grid from a Domain object. """
        lat = self._filter_domain_dimension(domain, ('latitude',))
        lat_n = abs(lat.start-lat.end)
        
        lon = self._filter_domain_dimension(domain, ('longitude',))
        lon_n = abs(lon.start-lon.end)

        cdms2_args = {
            'startLat': lat.start,
            'nlat': lat_n,
            'deltaLat': lat.step,
            'startLon': lon.start,
            'nlon': lon_n,
            'deltaLon': lon.step,
        }

        return cdms2.createUniformGrid(**cdms2_args)

    def _filter_domain_dimension(self, domain, targets):
        """ Filters a domains dimensions. """
        values = [x for x in domain.dimensions if x.name in targets]

        if not len(values):
            raise WPSServerError('Did not find a dimension named %s in domain %s',
                                 targets, domain)
        return values[0]

    def output_file(self, mime_type):
        """ Returns path to a valid output file. """
        if not os.path.exists(PYWPS_OUTPUT):
            os.mkdir(PYWPS_OUTPUT)

        ext = mimetypes.guess_extension(mime_type)

        out_file_path = os.path.join(PYWPS_OUTPUT, '%s%s' % (str(uuid()), ext))

        return out_file_path

    def update_status(self, message, progress):
        """ Updates process status. """
        logger.info('Status %s' % (message,))

        self.status.set(message, progress)

    def process_output(self, file_path):
        """ Creates variable to set process output. """
        mime_type, _ = mimetypes.guess_type(file_path)

        file_name = os.path.split(file_path)[1]

        format_args = {
            'hostname': settings.DAP_HOSTNAME,
            'port': ':%s' % (settings.DAP_PORT,) if settings.DAP_PORT else '',
            'filename': file_name,
        }

        output_var = Variable(settings.DAP_PATH_FORMAT.format(**format_args),
                              self._variable[0].var_name,
                              domains = self._variable[0].domains,
                              mime_type = mime_type)

        temp_file = NamedTemporaryFile(delete=False)

        temp_file.write(json.dumps(output_var.parameterize()))
        
        self.setOutputValue('output', temp_file.name)

    def __call__(self):
        """ Raises error when subclass has not overridden __call__. """
        raise WPSServerError('%s must implement __call__ function.' %
                             (self.identifier,))

    def get_input(self):
        """ Gets inputs for current process. """
        return [self._symbols[x.name] for x in self._operation[0].inputs]

    def get_parameter(self, name):
        """ Gets parameter by name. """
        return self._symbols[name]

    def execute(self):
        """ Called by Pywps library when process is executing. """
        logger.info('Executing process %s' % self.identifier)

        self._load_data()

        logger.info('Finished loading data')

        logger.info('Beginning execution')

        self()

        logger.info('Finished execution')
