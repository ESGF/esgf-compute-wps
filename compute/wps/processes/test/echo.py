from wps import logger
from wps.processes.data_manager import DataManager
from wps.processes.esgf_operation import ESGFOperation

from esgf import Variable

from pywps import config

import os

from uuid import uuid4 as uuid

class EchoOperation(ESGFOperation):
    def __init__(self):
        super(EchoOperation, self).__init__()

    @property
    def title(self):
        return 'Test Echo'

    def __call__(self, operations):
        operation = operations[0]

        data_manager = DataManager()

        output_path = config.getConfigValue('server', 'outputPath', '/var/wps')

        output_name = '%s.json' % (str(uuid()),)

        output_file = os.path.join(output_path, output_name)

        data_manager.write(output_file, operation.parameterize())

        output_var = Variable(output_file, '')

        self.complete_process(output_var)
