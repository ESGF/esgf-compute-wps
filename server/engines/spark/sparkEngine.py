from engines.registry import Engine
from engines.kernels.manager import kernelMgr
import os, logging
from engines.utilities import wpsLog

class SparkEngine(Engine):

    def execute( self, run_args ):
        result = kernelMgr.run( run_args )
        return result




        # dset = cleanup_url( data['url'] )
        # varname = data['id']
        # wpsLog.debug( "<<<<<Spark>>>>>--> Reading variable %s from dset %s" % ( str(varname), str(dset) ) )
        #
        # def loadPartition( partition_index ):
        #     f=cdms2.open( dset )
        #     variable = f[ varname ]
        #     return variable()
        #
        # def analytics( dataSlice ):
        #     variable = { 'data': dataSlice }
        #     kernel = TimeseriesAnalytics( variable )
        #     return kernel.execute( operation, region )
        #
        # partitions = sc.parallelize( range(num_parts) )
        # cdmsData = partitions.map(loadPartition)
        # result = cdmsData.map( analytics ).collect()
        # return result
        #
        #
