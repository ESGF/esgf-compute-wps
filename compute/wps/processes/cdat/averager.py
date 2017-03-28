import os
import uuid

import cdms2
import cdutil
from pywps import config
import esgf

from wps import logger
from wps.processes import esgf_operation

class CDUtilOperation(esgf_operation.ESGFOperation):
    def __init__(self):
        super(CDUtilOperation, self).__init__()

    @property
    def title(self):
        return 'CDUtil Averager'

    def __call__(self, data_manager, status):
        axes = self.parameter('axes')

        var = data_manager.open(self.input()[0])

        var_axes = var.cdms_variable.getAxisIds()

        try:
            axes_arg = ''.join([str(var_axes.index(x)) for x in axes.values])
        except ValueError as e:
            raise esgf.WPSServerError('Dimension %s of variable dimensions %s' % (e.message, var_axes))

        avg = cdutil.averager(var.cdms_variable, axis=axes_arg)

        output_path = config.getConfigValue('server', 'outputPath', '/var/wps')
        new_file_name = '%s.nc' % (str(uuid.uuid4()),)
        new_path = os.path.join(output_path, new_file_name)

        fout = cdms2.open(new_path, 'w')

        fout.write(avg, id=var.cdms_variable.id)

        fout.close()

        dap_url = self.create_dap_url(new_file_name)

        self.set_output(dap_url, var.cdms_variable.id)
