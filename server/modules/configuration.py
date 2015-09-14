CDAS_APPLICATION = 'CreateV'

CDAS_DEFAULT_DECOMP_STRATEGY = 'space.lon'

CDAS_DATA_PERSISTENCE_ENGINE = 'disk.numpy'
CDAS_PERSISTENCE_DIRECTORY = "~/.cdas/persistence"

CDAS_DEFAULT_NUM_NODES = 1

CDAS_STAGING = 'local'
#CDAS_STAGING = 'celery'

# Should be 'memory' for Flask and 'disk' for pyWPS/Django
CDAS_STATUS_CACHE_METHOD = 'memory'

#CDAS_COMPUTE_ENGINE = 'celery'
CDAS_COMPUTE_ENGINE = 'multiproc'
#CDAS_COMPUTE_ENGINE = 'spark'
#CDAS_COMPUTE_ENGINE = 'serial'

CDAS_NUM_WORKERS = 4

CDAS_DATA_CACHE = 'default'

CDAS_CELERY_BACKEND = 'redis'
# Application definition

CDAS_COLLECTIONS = [ ('MERRA/mon/atmos',       [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos' ] ),
                     ('CFSR/mon/atmos',        [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos' ] ),
                     ('ECMWF/mon/atmos',       [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos' ] ),
                   ]

MERRA_TEST_VARIABLES = {"collection": "MERRA/mon/atmos", "vars": [ "hur", "clt", "ua" ] }






