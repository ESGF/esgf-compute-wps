"""
ESGFProcess module.
"""

from pywps import config
from pywps.Process import WPSProcess

from wps import logger
from wps.conf import settings

from esgf import Domain
from esgf import Variable
from esgf import Operation
from esgf import WPSServerError

import cdms2

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)

import os
import sys
import json
import traceback

from tempfile import NamedTemporaryFile

from uuid import uuid4 as uuid

class ESGFProcess(WPSProcess):
    """ ESGFProcess

    Wrapper class to expose operations as a WPS process.
    """
    def __init__(self, operation):

        """ ESGFProcess init. """
        WPSProcess.__init__(
            self,
            operation.identifier,
            operation.title,
            abstract=operation.abstract,
            version=operation.version,
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

        self._operation = operation

    def _read_input(self, identifier):
        """ Reads a WPS input. """
        temp_path = self.getInputValue(identifier)

        with open(temp_path, 'r') as temp_file:
            input_value = temp_file.readlines()

        return input_value[0]

    def _load_from_input(self, identifier, output_type):
        """ Loads list of output_type from a WPS input. """
        input_json = self._read_input(identifier)

        input_obj = json.loads(input_json)

        return [output_type.from_dict(x) for x in input_obj]

    def _staging(self):
        """ WPS Process Staging

        Rebuilds the operations passed to the WPS process.
        """
        domains = {}
        variables = {}

        logger.info('Beginning staging of processes %s', self._operation.identifier)

        for domain in self._load_from_input('domain', Domain):
            domains[domain.name] = domain

        for variable in self._load_from_input('variable', Variable):
            variables[variable.name] = variable

        operations = self._load_from_input('operation', Operation)

        logger.info('Rebuilding variables')

        for _, variable in variables.iteritems():
            if variable.domains:
                variable.domains = [domains[x] for x in variable.domains]

        logger.info('Rebuilding operations')

        for operation in operations:
            operation.inputs = [variables[x.name] for x in operation.inputs]

            for name, param in operation.parameters.iteritems():
                if name == 'gridder':
                    param.grid = domains[param.grid]

        return operations

    def complete_process(self, variable):
        """ Signals the end of a process and the populates the output. """
        _, filename = os.path.split(variable.uri)

        dap_args = {
            'filename': filename,
            'hostname': settings.DAP_HOSTNAME,
            'port': ':%s' % (settings.DAP_PORT,) if settings.DAP_PORT else '',
        }

        variable.uri = settings.DAP_PATH_FORMAT.format(**dap_args)

        with NamedTemporaryFile(delete=False) as temp_file:
            json.dump(variable.parameterize(), temp_file)

            self.setOutputValue('output', temp_file.name)

    def update_status(self, message, progress=0.0):
        """ Propagates a status message. """
        self.status.set(message, progress)

    def execute(self):
        """ execute override

        Called by WPS process.
        """ 
        try:
            operations = self._staging()

            self._operation.complete_process = self.complete_process
            self._operation.status = self.update_status

            self._operation(operations)

            self._operation.status = None
            self._operation.complete_process = None
        except Exception as e:
            traceback.print_exc()
            e.message = 'Process failed "%s"' % (e.message,)
            raise
