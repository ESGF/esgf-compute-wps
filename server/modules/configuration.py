CDAS_APPLICATION = 'CreateV'

CDAS_DEFAULT_DECOMP_STRATEGY = 'space.lon'

CDAS_DATA_PERSISTENCE_ENGINE = 'disk.numpy'
CDAS_PERSISTENCE_DIRECTORY = "~/.cdas/persistence"

CDAS_DEFAULT_NUM_NODES = 1

CDAS_STAGING = 'local'
#CDAS_STAGING = 'celery'

#CDAS_COMPUTE_ENGINE = 'celery'
CDAS_COMPUTE_ENGINE = 'multiproc'
#CDAS_COMPUTE_ENGINE = 'spark'
#CDAS_COMPUTE_ENGINE = 'serial'

CDAS_NUM_WORKERS = 4

CDAS_DATA_CACHE = 'default'

CDAS_CELERY_BACKEND = 'redis'
# Application definition

CDAS_COLLECTIONS = [ ('MERRA/mon/atmos',       { 'type':'dods', 'url':'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos', 'open':['hur'] } ),
                     ('CFSR/mon/atmos',        { 'type':'dods', 'url':'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos' } ),
                     ('ECMWF/mon/atmos',       { 'type':'dods', 'url':'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos' } ),
                     ('MERRA/mon/atmos/hur',   { 'type':'file', 'url':'/usr/local/web/WPCDAS/data/hur.nc' } ),
                   ]

MERRA_TEST_VARIABLES = {"collection": "MERRA/mon/atmos", "vars": [ "hur", "clt", "ua" ] }

if __name__ == '__main__':
    import cdms2,os
    varname = 'hur'
    url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos/%s.ncml" % varname
    print "Downloading  variable '%s', url= %s " % ( varname, url )
    f = cdms2.open(url)
    f1 = cdms2.open(os.path.expanduser( '~/%s.nc' % varname), 'w'  )
    print "Opened file for variable '%s', reading data" % varname
    v = f(varname,level=(100000.0))
    print "Done reading data, now writing."
    f1.write(v)
    f1.close()
    f.close()
    print "Finished writing data"






