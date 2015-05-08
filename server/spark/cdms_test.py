import os, traceback, sys
import time
import cdms2, logging, pprint
import numpy as np
import numpy.ma as ma
import cdutil
import cPickle as pickle

def setClassPath():
    oldClassPath = os.environ.get('SPARK_CLASSPATH', '')
    cwd = os.path.dirname(os.path.realpath(__file__))
    os.environ['SPARK_CLASSPATH'] = cwd + ":" + oldClassPath

def getLogger( name, level=logging.DEBUG ):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if len( logger.handlers ) == 0:
        logger.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', '%s.log' % name ) ) ) )
    return logger

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))
wpsLog = logging.getLogger('wps')
wpsLog.setLevel(logging.DEBUG)
wpsLog.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'spark.log') ) ) )

pp = pprint.PrettyPrinter(indent=4)

dsets = [   { 'url': '/usr/local/web/data/MERRA/Temp2D/MERRA_3Hr_Temp.xml', 'id': 't' },
            { 'url': '/usr/local/web/data/MERRA/u750/merra_u750.xml', 'id': 'u', 'start': 1979 },
            { 'url': '/usr/local/web/data/MERRA/u750/merra_u750.nc', 'id': 'u' },
            { 'url': '/usr/local/web/data/MERRA/u750/merra_u750_1979_1982.nc', 'id': 'u' }  ]
var_index = 1

location = {'latitude': -40.0, 'longitude': 50.0}
num_months = 12
inputSpec = dsets[var_index]

def loadPartition( partition_index ):
    tN0 = time.time()
    f=cdms2.open( inputSpec['url'] )
    variable = f[ inputSpec['id'] ]
    data_start_year = inputSpec['start']
    start_year = data_start_year + partition_index / 12
    start_month = (partition_index % 12) + 1
    end_year = data_start_year + (partition_index + 1) / 12
    end_month = ( (partition_index + 1) % 12 ) + 1
    rdd_data = variable( time=( '%d-%d'%(start_year,start_month), '%d-%d'%(end_year,end_month), 'co') )
    tN1 = time.time()
    print "NetCDF load in %.4f sec" % ( tN1-tN0 )
    return rdd_data

def timeseries_average( data_slice ):
#    logger = getLogger( "log-map" )
    lat, lon = location['latitude'], location['longitude']
    timeseries = data_slice(latitude=(lat, lat, "cob"), longitude=(lon, lon, "cob"))
    result =  cdutil.averager( timeseries, axis='t', weights='equal' ).squeeze().tolist()
#    if logger: logger.debug( "Result = %s", str(result) )
    return result

if __name__ == "__main__":
    do_pickle = True
    t0 = time.time()
    rdd = loadPartition( 0 )
    if do_pickle:
        rdds = pickle.dumps( rdd.getValue() )
        tp0 = time.time()
        test = pickle.loads(rdds)
        tp1 = time.time()
        print "Pickle load in %.4f sec" % ( tp1-tp0 )
    ave = timeseries_average( rdd )
    t1 = time.time()
    print "Result: %s " % str(ave)
    print "Done in %.4f sec" % ( t1-t0 )

