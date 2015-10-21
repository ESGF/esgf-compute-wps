import traceback
import numpy.ma as ma
import numpy as np
import cdutil

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
                    if 'e' in axis:
                        axis1 = axis.replace('e','')
                        for input_variable in input_variables:
                            lresult = cdutil.averager( input_variable, axis=axis1 ) if axis1 else cdutil.averager( input_variable )
                            result = lresult if result is None else result + lresult
                        result /= len( input_variables )
                    else:
                        result = cdutil.averager( input_variable, axis=axis )

        except Exception, err:
            wpsLog.debug( "Exception applying Operation '%s':\n %s" % ( str(operation), traceback.format_exc() ) )

        t1 = time.time()
        wpsLog.debug( " $$$ Applied Operation: %s to variable shape %s in time %.4f, result shape = %s" % ( str( operation ), str(input_variable.shape), (t1-t0), result.shape  ) )
        return result, result_mdata

if __name__ == "__main__":
    import cdms2, sys
    from datacache.data_collections import getCollectionManger
    cm = getCollectionManger()
    collections = [ "MERRA/mon/atmos", "CFSR/mon/atmos" ]
    var = "ta"
    dsets = [ cdms2.open( cm.getURL( collection, var ) ) for collection in collections ]
    input_variables = [ dset( var, level=slice(0, 1, None) ) for dset in dsets ]
    result = None
    for input_variable in input_variables:
        lresult = cdutil.averager( input_variable, axis='xy' )
        result = lresult if result is None else result + lresult
    result /= len( input_variables )

    print result