import time

from wps import logger
from wps.processes import esgf_operation

class SleepOperation(esgf_operation.ESGFOperation):
    def __init__(self):
        super(SleepOperation, self).__init__()

    @property
    def title(self):
        return 'Test Sleep'

    def __call__(self, data_manager, status):
        for x in range(60):
            time.sleep(1)
            
            message = '%s out of 60' % (x+1,)

            status(message, (x+1)*100/60)

        self.set_output('', '')
