"""
CDMS2 MV Averager module.
"""
from ..esgf_process import ESGFProcess

from pywps import config

import os
import cdms2

class CDMS2(ESGFProcess):
    """ Averager class. 
   
    Averages variable over multiple dimensions.
    """
    def __init__(self):
        """Process initialization"""
        ESGFProcess.__init__(
            self,
            'averager', 
            'Averager')

    def __call__(self, v0, axes):
        """ Averages a variable over multiple dimensions. """
        axis_list = v0.getAxisList()
        axes_map = {}

        out_file = self.output_file()

        new_nc = cdms2.open(out_file, 'w') 

        for axis in axes:
            id_axis = self._find_axis_index(lambda x: x.id, axis_list, axis)

            new_var = cdms2.MV.average(v0, axis=id_axis[0][1])

            new_nc.write(new_var, id='avg_%s' % (axis,))
    
        new_nc.close()

        return out_file

    def _find_axis_index(self, func, axes, value):
        """ Helper function to translate cdms2 indexes. """
        return [(func(axes[x]), x) for x in range(len(axes))
                if func(axes[x]) == value]
