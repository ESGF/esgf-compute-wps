import numpy as np
import time


def tile( tseries, data_array, time_index ):
    out = np.tile( tseries, data_array.shape[1:] )
    return out

if __name__ == '__main__':
    data_shape = (1000, 200, 200 )
    time_index = 0
    tseries = np.array( range( 0, data_shape[time_index] ) )
    data_array = np.zeros( data_shape )
    bcast_shape = [1]*len(data_array.shape)
    bcast_shape[time_index] = tseries.shape[0]
    tseries = tseries.reshape( bcast_shape )


    t0 = time.time()
    out = tile( tseries, data_array, time_index )
    t1 = time.time()
    print "Completed tile op in %.2f sec, result = \n%s" % ( ( t1-t0), out )
