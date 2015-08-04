CDAS_APPLICATION = 'CreateV'

CDAS_STAGING = 'local'

CDAS_DEFAULT_DECOMP_STRATEGY = 'space.lon'

CDAS_DEFAULT_NUM_NODES = 1
#CDAS_STAGING = 'celery'

CDAS_COMPUTE_ENGINE = 'celery'
#CDAS_COMPUTE_ENGINE = 'spark'
#CDAS_COMPUTE_ENGINE = 'serial'

CDAS_DATA_CACHE = 'default'

CDAS_CELERY_BROKER = 'amqp://guest@localhost//'
CDAS_CELERY_BACKEND = 'amqp'
# Application definition

CDAS_COLLECTIONS = [ ('MERRA/mon/atmos',       [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos' ] ),
                     ('CFSR/mon/atmos',        [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos' ] ),
                     ('ECMWF/mon/atmos',       [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos' ] ),
                   ]



