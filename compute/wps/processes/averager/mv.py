"""
CDMS2 MV Averager module.
"""
from wps import logger
from ..esgf_process import ESGFProcess

from pywps import config

from cdutil import averager

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

    def __call__(self, v0, axes):
        """ Averages a variable over multiple dimensions. """
        out_file = self.output_file('application/x-netcdf')

        new_nc = cdms2.open(out_file, 'w') 
        
        axis = ''.join(str(v0.getAxisIndex(x)) for x in axes)

        new_var = averager(v0, axis=axis)

        new_nc.write(new_var, id='avg_%s' % ('_'.join(axes),))
    
        new_nc.close()

        self.process_output(out_file)

    def _find_axis_index(self, func, axes, value):
        """ Helper function to translate cdms2 indexes. """
        return [(func(axes[x]), x) for x in range(len(axes))
                if func(axes[x]) == value]
