from ..esgf_process import ESGFProcess

import time

class SleepProcess(ESGFProcess):
    def __init__(self):
        ESGFProcess.__init__(self, 'Sleep')

    def __call__(self, *args, **kwargs):
        output_file = self.output_file('text/plain')

        for x in range(30):
            time.sleep(1)

            self.status.set('Sleeping %s' % x, x*100.0/30.0)            

        self.process_output(output_file)
