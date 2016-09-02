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
from esgf import WPSServerError

from uuid import uuid4 as uuid

from tempfile import NamedTemporaryFile

import os
import sys
import json
import cdms2
import types

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

        self.addLiteralInput(
            'operation',
            'Operation',
            'Operation/Arguments for the process.',
            uoms=(),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=('*'),
            type=types.StringType,
            default=None,
            metadata=[])

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

        self._operation = None
        self._variable = None
        self._domains = None

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
        op_str = self._read_input_literal('operation')

        self._operation = Operation.from_str(self.identifier, op_str)

    def _load_variable(self):
        """ Loads the variable to be processed. """
        var_str = self._read_input('variable')

        var = Variable.from_dict(json.loads(var_str[0]))

        self._variable = var 

    def _load_domains(self):
        """ Loads the domains that will be used in the process. """
        dom_str = self._read_input('domain')

        dom = Domain.from_dict(json.loads(dom_str[0]))

        self._domains = self._symbols[dom.name] = dom

    def _cdms2_selector_value(self, dim):
        """ Creates the value for a CDMS2 selector. """
        if not dim.end:
            return dim.start

        return (dim.start, dim.end)

    def _cdms2_selector(self):
        """ Creates a CDMS2 selector from the variables domain. """
        domain = self._symbols[self._variable.domains]

        selector = {}

        for dim in domain.dimensions:
            selector[dim.name] = self._cdms2_selector_value(dim)

        return selector

    def _load_data(self):
        """ Loads all the required data for the process. """
        self._load_operation()

        self._load_domains()

        self._load_variable()

        # TODO dynamic reader dependent on mime-type
        file_obj = cdms2.open(self._variable.uri, 'r')

        selector = self._cdms2_selector()

        var = file_obj(self._variable.var_name, **selector)

        self._symbols[self._variable.name] = var

    def output_file(self):
        """ Returns path to a valid output file. """
        out_path = config.getConfigValue('server', 'outputPath', '/var/wps')

        if not os.path.exists(out_path):
            os.mkdir(out_path)

        out_file_path = os.path.join(out_path, '%s.nc' % (str(uuid()),))

        return out_file_path

    def __call__(self, *arg, **kwarg):
        """ Raises error when subclass has not overridden __call__. """
        raise WPSServerError('%s must implement __call__ function.' %
                             (self.identifier,))

    def execute(self):
        """ Called by Pywps library when process is executing. """
        self._load_data()
    
        args_list = [arg for arg in self._operation.parameters
                        if not isinstance(arg, NamedParameter)]
        
        kwargs_list = [arg for arg in self._operation.parameters
                          if arg not in args_list]

        args = [self._symbols[arg.name] for arg in args_list]
        kwargs = dict([(arg.name, arg.values) for arg in kwargs_list])

        output_file_paths = self(*args, **kwargs)

        out_var = Variable(output_file_paths, self._variable.var_name, name='')

        temp_file = NamedTemporaryFile(delete=False)

        temp_file.write(json.dumps(out_var.parameterize()))

        self.setOutputValue('output', temp_file.name)
