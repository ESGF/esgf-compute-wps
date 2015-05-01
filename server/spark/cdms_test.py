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
    logger.addHandler( logging.FileHandler( '/usr/local/web/WPCDAS/server/logs/%s.log' % name ) )
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
            { 'url': '/usr/local/web/data/MERRA/u750/merra_u750.xml', 'id': 'u' },
            { 'url': '/usr/local/web/data/MERRA/u750/merra_u750.nc', 'id': 'u' },
            { 'url': '/usr/local/web/data/MERRA/u750/merra_u750_1979_1982.nc', 'id': 'u' }  ]
var_index = 0

def compute_anomaly(x) :
    return np.ma.anomalies(x)

if __name__ == "__main__":
    setClassPath()
    dset = dsets[ var_index ]

    f=cdms2.open( dset['url'] )
    variable = f[ dset['id'] ]

    val = variable
    data_string = pickle.dumps(val)
    new_val = pickle.loads(data_string)

    diff = new_val - val
    print "\n ---------- Serialize error: ---------- "
    pp.pprint( diff )

    # read_start_time = time.time()
    # timeseries = variable( latitude = ( -40.0, -40.0, "cob"), longitude= (50.0, 50.0, "cob") ).squeeze()
    # read_end_time = time.time()
    # wpsLog.debug( " $$$ DATA READ Complete: " + str( (read_end_time-read_start_time) ) )
    #
    # result = compute_anomaly( timeseries )
    #
    # print "\n ---------- Input: ---------- "
    # pp.pprint(result)

