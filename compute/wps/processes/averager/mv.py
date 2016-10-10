from wps import logger
from wps.processes.data_manager import DataManager
from wps.processes.esgf_operation import ESGFOperation

from pywps import config

from esgf import Variable
from esgf import WPSServerError

from cdutil import averager

import os

from tempfile import NamedTemporaryFile

from uuid import uuid4 as uuid

class CDUtilOperation(ESGFOperation):
    def __init__(self):
        super(CDUtilOperation, self).__init__()

    @property
    def title(self):
        return 'CDUtil Averager'

    def __call__(self, operations, auth):
        operation = operations[0]

        data_manager = DataManager()
        
        try:
            gridder = operation.parameters['gridder']
        except KeyError:
            gridder = None

        with NamedTemporaryFile() as pem_file:
            pem_file.write(str(auth['pem']))
            pem_file.flush()

            metadata = {
                'gridder': gridder,
                'cert': pem_file.name,
                'temp': config.getConfigValue('server', 'tempPath', '/tmp/wps'),
            }

            data = data_manager.read(operation.inputs[0], **metadata)

        try:
            axes = operation.parameters['axes']
        except KeyError:
            raise WPSServerError('Expecting parameter named axes')

        input_var = data.chunk

        axes_arg = ''.join((str(input_var.getAxisIndex(x)) for x in axes.values))

        new_var = averager(input_var, axis=axes_arg)

        new_var_name = '%s_avg_%s' % (input_var.id, '_'.join(axes.values))

        new_file_name = '%s.nc' % (str(uuid()),)

        output_path = config.getConfigValue('server', 'outputPath', '/var/wps')

        new_file = os.path.join(output_path, new_file_name)

        data_manager.write('file://' + new_file, new_var, id=new_var_name)

        out_var = Variable(new_file, new_var_name)

        self.complete_process(out_var)
