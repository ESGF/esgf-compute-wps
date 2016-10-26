import datetime
import os
import tempfile
import uuid

import cdutil
from pywps import config
import esgf

from wps.processes import data_manager
from wps.processes import esgf_operation

class CDUtilOperation(esgf_operation.ESGFOperation):
    def __init__(self):
        super(CDUtilOperation, self).__init__()

    @property
    def title(self):
        return 'CDUtil Averager'

    def __call__(self, auth, status):
        dm = data_manager.DataManager()
        
        gridder = self.parameter('gridder', False)

        with tempfile.NamedTemporaryFile() as pem_file:
            pem_file.write(str(auth['pem']))
            pem_file.flush()

            metadata = {
                'gridder': gridder,
                'cert': pem_file.name,
                'temp': config.getConfigValue('server', 'tempPath', '/tmp/wps'),
            }

            data = dm.read(self.input()[0], **metadata)

        axes = self.parameter('axes')

        status('Read input data')

        input_var = data.chunk

        axes_arg = ''.join((str(input_var.getAxisIndex(x)) for x in axes.values))

        status('Averagering "%s" over dimensions "%s"' %
               (input_var.id,
                ', '.join(axes.values)))
        
        start = datetime.datetime.now() 

        new_var = cdutil.averager(input_var, axis=axes_arg)

        status('Finished averaging in %s' % (datetime.datetime.now()-start,))

        new_var_name = '%s_avg_%s' % (input_var.id, '_'.join(axes.values))

        new_file_name = '%s.nc' % (str(uuid.uuid4()),)

        output_path = config.getConfigValue('server', 'outputPath', '/var/wps')

        new_file = os.path.join(output_path, new_file_name)

        status('Writing output to "%s"' % (new_file,))

        dm.write('file://' + new_file, new_var, id=new_var_name)

        self.set_output(new_file, new_var_name)

