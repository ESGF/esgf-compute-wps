CDAS_APPLICATION = 'CreateV'

CDAS_DEFAULT_DECOMP_STRATEGY = 'space.lon'

CDAS_DEFAULT_NUM_NODES = 1

CDAS_STAGING = 'local'
#CDAS_STAGING = 'celery'

CDAS_COMPUTE_ENGINE = 'celeryEngine'
#CDAS_COMPUTE_ENGINE = 'sparkEngine'
#CDAS_COMPUTE_ENGINE = 'serialEngine'

CDAS_DATA_CACHE = 'default'

CDAS_CELERY_BACKEND = 'redis'
# Application definition

CDAS_COLLECTIONS = [ ('MERRA/mon/atmos',       [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos' ] ),
                     ('CFSR/mon/atmos',        [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos' ] ),
                     ('ECMWF/mon/atmos',       [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos' ] ),
                   ]

MERRA_TEST_VARIABLES = {"collection": "MERRA/mon/atmos", "vars": [ "hur", "clt", "ua" ] }






