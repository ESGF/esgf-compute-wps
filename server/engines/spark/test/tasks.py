import logging
import os

from pyspark import SparkContext

num_parts = 1
wpsLog = logging.getLogger('wps')
wpsLog.setLevel(logging.DEBUG)
if len( wpsLog.handlers ) == 0:
    wpsLog.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '../../../celery', '..', 'logs', 'wps.log' ) )))

sc = SparkContext('local[%d]' % num_parts, "cdaSpark")
partitions = sc.parallelize( range(num_parts) )

def cleanup_url( url ):
    if url.startswith('file://'): url = url[6:]
    return url

def submitTask( run_args ):
    engine_id = run_args['engine']
    engine = submitTask.engines.getInstance( engine_id )
    wpsLog.info( " Celery submit task, args = '%s', engine = %s (%s)" % ( str( run_args ), engine_id, type(engine) ) )
    result =  engine.execute( run_args )
    return result

