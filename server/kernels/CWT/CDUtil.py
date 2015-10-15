import traceback

import numpy.ma as ma
import numpy as np
import cdutil
import cdms2, sys
from modules.utilities import *
from kernels.cda import CDASKernel

class Averages( CDASKernel ):


    def applyOperation( self, input_variables, operation ):
        result = None
        result_mdata = {}
        t0 = time.time()
        input_variable = input_variables[0]
        try:
            if operation is not None:
                method = operation.get('method','').lower()
                if method == 'average':
                    axis = operation.get('axis','xy').lower()
                    result = cdutil.averager( input_variable, axis=axis )

        except Exception, err:
            wpsLog.debug( "Exception applying Operation '%s':\n %s" % ( str(operation), traceback.format_exc() ) )

        t1 = time.time()
        wpsLog.debug( " $$$ Applied Operation: %s to variable shape %s in time %.4f, result shape = %s" % ( str( operation ), str(input_variable.shape), (t1-t0), result.shape  ) )
        return result, result_mdata

