from pyspark import SparkContext
import cdms2, logging, os
from engines.kernels.timeseries_analysis import TimeseriesAnalytics
num_parts = 1
wpsLog = logging.getLogger('wps')
wpsLog.setLevel(logging.DEBUG)
if len( wpsLog.handlers ) == 0:
    wpsLog.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'wps.log' ) )))

sc = SparkContext('local[%d]' % num_parts, "cdaSpark")
partitions = sc.parallelize( range(num_parts) )

def cleanup_url( url ):
    if url.startswith('file://'): url = url[6:]
    return url

def submitSparkTask( data, region, operation ):
    dset = cleanup_url( data['url'] )
    varname = data['id']
    wpsLog.debug( "<<<<<Spark>>>>>--> Reading variable %s from dset %s" % ( str(varname), str(dset) ) )

    def loadPartition( partition_index ):
        f=cdms2.open( dset )
        variable = f[ varname ]
        return variable()

    def analytics( dataSlice ):
        variable = { 'data': dataSlice }
        kernel = TimeseriesAnalytics( variable )
        return kernel.execute( operation, region )

    partitions = sc.parallelize( range(num_parts) )
    cdmsData = partitions.map(loadPartition)
    result = cdmsData.map( analytics ).collect()
    return result

