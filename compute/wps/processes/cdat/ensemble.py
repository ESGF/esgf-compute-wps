import cdms2
import os
from pywps import config
import uuid

from wps import logger
from wps.processes import esgf_operation

class CDATEnsemble(esgf_operation.ESGFOperation):
    def __init__(self):
        super(CDATEnsemble, self).__init__()

    @property
    def title(self):
        return 'CDAT Ensemble'

    def __call__(self, data_manager, status):
        src = [data_manager.metadata(x) for x in self.input()]
        
        denom = len(src)

        time_dim = src[0].getTime()

        output_path = config.getConfigValue('server', 'outputPath', '/var/wps')

        new_file_name = str(uuid.uuid4())

        new_file = os.path.join(output_path, new_file_name)

        var_name = self.input()[0].var_name

        file_out = cdms2.open(new_file, 'w')        

        file_out.copyAxis(src[0].getAxis(0))

        for t in time_dim:
            data = [x(time=t) for x in src]

            data_sum = reduce(lambda x, y: x + y, data)

            data_sum /= denom

            file_out.write(data_sum, id=var_name)

        file_out.close() 

        self.set_output(new_file, '%s_avg' % (var_name,))
