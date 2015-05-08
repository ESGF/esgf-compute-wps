from tasks import timeseries, createDomain, addVariable
import time
from utilities import Profiler
profiler = Profiler()

varId = 'u'
domainSpec = { 'id': 'merra_u750' }
varSpec = {'dset': '/usr/local/web/data/MERRA/u750/merra_u750_1979_1982.nc', 'id': varId }
location = {'latitude': -40.0, 'longitude': 50.0}
op = ''
profiler.mark()

response = createDomain.delay( domainSpec )
domainId = response.get()
profiler.mark('createDomain')

response = addVariable.delay( domainId, varSpec )
variableId = response.get()
profiler.mark('addVariable')

#response = timeseries.delay( variableId, domainId, location, op )
#result = response.get()
#profiler.mark('timeseries')

profiler.dump( " Received Result=%s with times:" % ( str( result ) ) )


