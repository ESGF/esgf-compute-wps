from modules import Executable
from engines.kernels.manager import kernelMgr
from modules.utilities import *

from modules.utilities import wpsLog
num_parts = 1

def cleanup_url( url ):
    if url.startswith('file://'): url = url[6:]
    return url

def generate_kernel_args( dataSlice, run_args ):
    run_args['dataSlice'] = dataSlice
    return run_args

def getSparkLogger():
    pass

class SparkEngine(Executable):
    sc = None
    partitions = None
    logger = None

    @classmethod
    def initContext(cls):
        from pyspark import SparkContext
        if cls.sc == None:
           cls.sc = SparkContext('local[%d]' % num_parts, "cdaSpark")
           cls.partitions = cls.sc.parallelize( range(num_parts) )

    @classmethod
    def initLogger(cls):
        import logging
        if cls.logger == None:
            cls.logger = logging.getLogger('cdas-spark')
            cls.logger.setLevel( logging.DEBUG )
            if len( cls.logger.handlers ) == 0:
                cls.logger.addHandler( logging.FileHandler( os.path.join( LogDir, 'spark.log' ) ) )

    def execute( self, task_request, run_args ):
        SparkEngine.initContext()
        wpsLog.info( "Executing Spark engine, args: %s" % ( run_args ) )
        data = get_json_arg( 'data', run_args )

        def loadPartition( partition_index ):
            import cdms2
            SparkEngine.initLogger()
            SparkEngine.logger.debug( " Reading variable: %s" % str(data) )

            dset_path = cleanup_url( data['url'] )
            varname = data['id']
            f=cdms2.open( dset_path )
            variable = f[ varname ]
            return variable()

        cdmsData = SparkEngine.partitions.map(loadPartition)

        def run_kernel( dataSlice ):
            kernel_args = generate_kernel_args( dataSlice, run_args )
            SparkEngine.logger.debug( " Running kernel, args: %s" % kernel_args )
            result = kernelMgr.run( kernel_args )
            return result

        result = cdmsData.map( run_kernel ).collect()
        return result