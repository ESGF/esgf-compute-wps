"""
ESGFProcess module.
"""

import json
import os
import sys
import tempfile
import uuid

import cdms2
import esgf
import pywps
from pywps import Process

from wps import conf
from wps import logger
from wps.conf import settings
from wps.processes import data_manager

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)

class ESGFProcess(Process.WPSProcess):
    """ ESGFProcess

    Wrapper class to expose operations as a WPS process.
    """
    def __init__(self, operation):

        """ ESGFProcess init. """
        Process.WPSProcess.__init__(
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

        self.addComplexInput(
            'auth',
            'Authentication',
            'Authentication info for the process.',
            metadata=[],
            minOccurs=0,
            maxOccurs=1,
            formats=[
                {'mimeType': 'text/json'},
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

        logger.info('Beginning staging of processes %s',
                    self._operation.identifier)

        for domain in self._load_from_input('domain', esgf.Domain):
            domains[domain.name] = domain

            logger.debug('Domain %r', domain)

        logger.info('Loaded %d domains', len(domains))
    
        for variable in self._load_from_input('variable', esgf.Variable):
            variables[variable.name] = variable

            logger.debug('Variable %r', variable)

        logger.info('Loaded %d variables', len(variables))

        operations = self._load_from_input('operation', esgf.Operation)

        logger.info('Operations %r', operations) 

        logger.info('Loaded %d operations', len(operations))

        # TODO try to rebuild tree/organize non-dependent operations

        logger.info('Rebuilding variables')

        for _, variable in variables.iteritems():
            if variable.domains:
                variable.domains = [domains[x] for x in variable.domains]

        logger.info('Rebuilding operations')

        for operation in operations:
            op_inputs = []

            for inp in operation.inputs:
                try:
                    op_inputs.append(variables[inp.name])
                except KeyError:
                    pass
                else:
                    continue

                candidates = [x for x in operations if x.name == inp.name]

                try:
                    op_inputs.append(candidates[0])
                except KeyError:
                    raise esgf.WPSServerError('Failed to find input "%s"' % (inp.name,))

            operation.inputs = op_inputs

            for name, param in operation.parameters.iteritems():
                if name == 'gridder':
                    logger.info('Rebuilding gridder')

                    param.grid = domains[param.grid]

            if operation.domain:
                operation.domain = domains[operation.domain.name]

            if self._operation.identifier == operation.identifier:
                self._operation.data = operation

        if not self._operation.data:
            raise esgf.WPSServerError('Failed to find operation')

        logger.info('Done staging')

    def complete_process(self, variable):
        """ Signals the end of a process and the populates the output. """
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            logger.info('Writing output "%r" to "%s"',
                        variable,
                        temp_file.name)

            json.dump(variable.parameterize(), temp_file)

            self.setOutputValue('output', temp_file.name)

    def update_status(self, message, progress=0.0):
        """ Propagates a status message. """
        logger.info('Status %d %s', progress, message)

        self.status.set(message, progress)

    def execute(self):
        """ execute override

        Called by WPS process.
        """ 
        try:
            self._staging()

            auth = json.loads(self._read_input('auth'))

            with data_manager.DataManager(ca_dir=auth['ca_dir'],
                                          pem=auth['pem']) as dm:
                self._operation(dm, self.update_status)

                self.complete_process(self._operation.output)
        except Exception as e:
            logger.exception('Operation failed: %s' % (e.message,))

            raise
