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
    import cdms2

    def get_one_month_subset( input_data, month_index, month_index_array ):   # Assumes time is dimension 0.
        im_mask = month_index_array <> month_index
        if input_data.ndim > 1:
            im_mask = np.tile( im_mask, input_data.shape[1:] )
        return ma.masked_array( input_data, mask = im_mask )

    def annual_cycle( input_variable ):
        time_vals = input_variable.getTime().asComponentTime()
        month_index_array = np.array( [  tv.month for tv in time_vals ] )
        squeezed_input = input_variable.squeeze()
        acycle = [ ma.average( get_one_month_subset( squeezed_input, month_index, month_index_array ), axis=0 ) for month_index in range(1,13) ]
        return ma.array(acycle)

    var = "ta"
    url = "http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos/ta.ncml"
    dset =  cdms2.open( url )
    input_variable = dset( var, level=700 )

    t0 = time.time()
    lresult = annual_cycle( input_variable ).squeeze()
    t1 = time.time()

    print "numpy Comp time: %.2f, shape = %s, sample values:" % ( ( t1-t0 ), lresult.shape )
    print lresult.flatten()[0:10]

    operator = cdutil.ANNUALCYCLE
    t0 = time.time()
    lresult = operator.climatology( input_variable ).squeeze()
    t1 = time.time()

    print "cdutil Comp time: %.2f, shape = %s, sample values:" % ( ( t1-t0 ), lresult.shape )
    print lresult.flatten()[0:10]

#    lresult = cdutil.averager( input_variable, axis='t' )
#    lresult = numpy.average( input_variable, axis=0 )
    # from datacache.data_collections import getCollectionManger
    # cm = getCollectionManger()
    # collections = [ "MERRA/mon/atmos", "CFSR/mon/atmos" ]
    # result = None
    # dsets = [ cdms2.open( cm.getURL( collection, var ) ) for collection in collections ]
    # input_variables = [ dset( var, level=slice(0, 1, None) ) for dset in dsets ]
    # for input_variable in input_variables:
    #     lresult = cdutil.averager( input_variable, axis='xy' )
    #     result = lresult if result is None else result + lresult
    # result /= len( input_variables )
    #
    # print result






