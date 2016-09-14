"""
CDMS2 MV Averager module.
"""
from wps import logger
from wps.processes.esgf_process import ESGFProcess

from esgf import WPSServerError

from pywps import config

from cdutil import averager

from datetime import datetime

import os
import cdms2

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)

class CDMS2Process(ESGFProcess):
    """ Averager class. 
   
    Averages variable over multiple dimensions.
    """
    def __init__(self):
        """Process initialization"""
        ESGFProcess.__init__(
            self,
            'UV-CDAT cdutil averager')

    def __call__(self):
        """ Averages a variable over multiple dimensions. """
        v0 = self.get_parameter('v0')

        axes = self.get_parameter('axes')

        axis = ''.join(str(v0.getAxisIndex(x)) for x in axes)

        self.update_status('Average over %r' % zip(axes, axis), 25.0)

        start = datetime.now()

        new_var = averager(v0, axis=axis)

        self.update_status('Finished average in %s' % (datetime.now()-start), 50.0)

        out_file = self.output_file('application/x-netcdf')

        new_nc = cdms2.open(out_file, 'w') 

        new_file_name = os.path.split(out_file)[1]

        self.update_status('Created output file %s' % (new_file_name), 75.0)

        new_nc.write(new_var, id='avg_%s' % ('_'.join(axes),))
    
        new_nc.close()

        self.update_status('Wrote new variable %s to %s' % ('_'.join(axes), new_file_name), 100.0)

        self.process_output(out_file)
