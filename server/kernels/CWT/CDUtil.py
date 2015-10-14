import traceback

import numpy.ma as ma
import numpy as np
import cdutil
import cdms2, sys
from modules.utilities import *
from kernels.cda import CDASKernel

class Averages( CDASKernel ):

    def run( self, subsetted_variables, metadata_recs, operation ):
        try:
            start_time = time.time()
            result  = self.applyOperation( subsetted_variables, operation )
            result_obj = self.getResultObject( metadata_recs, result  )
            end_time = time.time()
            wpsLog.debug( "Computed operation %s on region %s: time = %.4f" % ( str(operation), str(result_obj["data.region"]), (end_time-start_time) ) )
        except Exception, err:
            wpsLog.debug( "Exception executing timeseries process:\n " + traceback.format_exc() )
        return result_obj

    def applyOperation( self, input_variables, operation ):
        result = None
        t0 = time.time()
        input_variable = input_variables[0]
        try:
            self.setTimeBounds( input_variable )
            if operation is not None:
                method = operation.get('method','').lower()
                if method == 'average':
                    axis = operation.get('axis','xy').lower()
                    result = cdutil.averager( input_variable, axis=axis )

        except Exception, err:
            wpsLog.debug( "Exception applying Operation '%s':\n %s" % ( str(operation), traceback.format_exc() ) )

        t1 = time.time()
        wpsLog.debug( " $$$ Applied Operation: %s to variable shape %s in time %.4f, result shape = %s" % ( str( operation ), str(input_variable.shape), (t1-t0), result.shape  ) )
        return result

