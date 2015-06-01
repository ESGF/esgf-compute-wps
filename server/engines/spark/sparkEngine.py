from engines.registry import Engine
from engines.kernels.manager import kernelMgr
from pyspark import SparkContext
from engines.utilities import wpsLog
num_parts = 1

def cleanup_url( url ):
    if url.startswith('file://'): url = url[6:]
    return url

sc = SparkContext('local[%d]' % num_parts, "cdaSpark")
partitions = sc.parallelize( range(num_parts) )

def generate_kernel_args(self, dataSlice, run_args ):
    run_args['data'] = dataSlice
    return run_args

class SparkEngine(Engine):

    def execute( self, run_args ):
        wpsLog.info( "Executing Spark engine, args: %s" % str(run_args))
        partitions = sc.parallelize( range(num_parts) )
        data = run_args['data']

        def loadPartition( partition_index ):
            dset = cleanup_url( data['url'] )
            varname = data['id']
            wpsLog.debug( "<<<<<Spark>>>>>--> Reading variable %s from dset %s" % ( str(varname), str(dset) ) )
            variable = dset[ varname ]
            return variable()

        cdmsData = partitions.map(loadPartition)

        def run_kernel( dataSlice ):
            kernel_args = generate_kernel_args( dataSlice, run_args )
            result = kernelMgr.run( kernel_args )
            return result

        result = cdmsData.map( run_kernel ).collect()
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
