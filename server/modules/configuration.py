CDAS_APPLICATION = 'CreateV'

CDAS_REDUCTION_STRATEGY = { 'DecompositionStrategy': { 'id':'space.lon' },
                            'DecimationStrategy':    { 'id':'time.subset', 'max_size': 600 }  }

CDAS_DATA_PERSISTENCE_ENGINE = 'disk.numpy'
CDAS_PERSISTENCE_DIRECTORY = "~/.cdas/persistence"

CDAS_DEFAULT_NUM_NODES = 1

CDAS_STAGING = 'local'
#CDAS_STAGING = 'celery'

CDAS_COMPUTE_ENGINE = 'mpi'

#CDAS_OUTGOING_DATA_DIR='/Developer/Projects/EclipseWorkspace/CreateV/source/climateinspector/web/data'
CDAS_OUTGOING_DATA_DIR='/usr/local/web/WPCDAS/clients/web/htdocs/data'
CDAS_OUTGOING_DATA_URL='http://localhost:8002/data/'

CDAS_NUM_WORKERS = 3

CDAS_DATA_CACHE = 'default'

CDAS_CELERY_BACKEND = 'redis'
# Application definition

CDAS_COLLECTIONS = [ ('MERRA/mon/atmos',       { 'type':'dods', 'url':'http://dptomcat01.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos/' } ),
                     ('CFSR/mon/atmos',        { 'type':'dods', 'url':'http://dptomcat01.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos' } ),
                     ('ECMWF/mon/atmos',       { 'type':'dods', 'url':'http://dptomcat01.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos' } ),
                     ('MERRA/6hr/atmos',       { 'type':'dods', 'url':'http://dptomcat01.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/6hr/atmos' } ),
                     ('CFSR/6hr/atmos',        { 'type':'dods', 'url':'http://dptomcat01.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/6hr/atmos' } ),
                     ('ECMWF/6hr/atmos',       { 'type':'dods', 'url':'http://dptomcat01.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/6hr/atmos' } ),
                     ('MERRA/mon/atmos/ta',    { 'type':'file', 'url':'/usr/local/web/WPCDAS/data/atmos_ta.nc' } ),
                     ('MERRA/mon/atmos/ua',    { 'type':'file', 'url':'/usr/local/web/WPCDAS/data/atmos_ua.nc' } ),
                     ]

MERRA_TEST_VARIABLES = {"collection": "MERRA/mon/atmos", "vars": [ "hur", "clt", "ua" ] }
MERRA_ENS_TEST_VARIABLES = {"collection": "MERRA/mon/atmos", "vars": [ "hur", "clt", "ua" ] }
MERRA_LOCAL_TEST_VARIABLES = {"collection": "MERRA/mon/atmos/ta", "vars": [ "ta" ] }
MERRA_HF_TEST_VARIABLES = {"collection": "MERRA/6hr/atmos", "vars": [ "va", "ta", "ua", "psl", "hus" ] }

if __name__ == '__main__':
    import cdms2, time
    varname = 'ta'
    url="http://dptomcat01.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/6hr/atmos/%s.ncml" % varname
    print "Downloading  variable '%s', url= %s " % ( varname, url )
    f = cdms2.open(url)
    t0 = time.time()
    v = f.variables[ varname ]
    print "Full Shape = ", str(v.shape)
    tv = v( cdms2.levelslice(20), cdms2.timeslice(0,-1,120))
    t1 = time.time()
    print "Slice Shape = ", str(tv.shape)
    print "Data = %s" % str( tv[0:3] )
    f.close()
    print "Data read in %.2f sec" % (t1-t0)






