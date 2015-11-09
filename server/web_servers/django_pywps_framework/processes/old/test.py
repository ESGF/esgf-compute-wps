from wps import settings
from staging import stagingRegistry
import time, json

handler = stagingRegistry.getInstance( settings.CDAS_STAGING  )
if handler is None:
     print " Staging method not configured. Using celery. "
     handler = stagingRegistry.getInstance( 'celery' )

data = {'url': '/att/nobackup/tpmaxwel/data/MERRA2_100.instM_3d_asm_Np.xml', 'id': 'U', 'start': 1991 }
#data = {'url': '/usr/local/web/data/MERRA/u750/merra_u750.xml', 'id': 'u', 'start': 1979}
region = {'latitude': -40.0, 'longitude': 50.0}
operation='timeseries'

t0 = time.time()
print " Running handler %s with engine %s. " % ( settings.CDAS_STAGING, settings.CDAS_COMPUTE_ENGINE )
result =  handler.execute( { 'data':data, 'region':region, 'operation':operation, 'engine': settings.CDAS_COMPUTE_ENGINE } )
result_json = json.dumps( result )
t1 = time.time()
print " $$$ CDAS Process (response time: %.3f sec): Result='%s' " %  ( (t1-t0), str( result_json ))