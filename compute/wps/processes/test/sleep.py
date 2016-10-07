from wps import logger
from wps.processes.esgf_operation import ESGFOperation

from esgf import Variable

from time import sleep

class SleepOperation(ESGFOperation):
    def __init__(self):
        super(SleepOperation, self).__init__()

    @property
    def title(self):
        return 'Test Sleep'

    def __call__(self, operations):
        for x in range(60):
            sleep(1)
            
            message = '%s out of 60' % (x+1,)

            self.status(message, (x+1)*100/60)

        self.complete_process(Variable('', ''))
