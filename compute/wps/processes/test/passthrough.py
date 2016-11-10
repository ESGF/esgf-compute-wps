import uuid

from pywps import config

from wps.conf import settings
from wps.processes import esgf_operation

class PassThrough(esgf_operation.ESGFOperation):
    def __init__(self):
        super(PassThrough, self).__init__()

    @property
    def title(self):
        return 'Test File Pass-Through'

    def __call__(self, data_manager, status):
        data = self.input()[0]

        var = data_manager.read(data)

        output_path = config.getConfigValue('server', 'outputPath', '/var/lib')

        output_filename = '%s.nc' % (str(uuid.uuid4()),)

        output_file = '%s/%s' % (output_path, output_filename)

        data_manager.write(output_file, var, data.var_name)

        dap_url = self.create_dap_url(output_filename)

        self.set_output(dap_url, data.var_name)
