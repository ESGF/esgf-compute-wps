from tasks import computeTimeseries, createDomain, addVariable
from utilities import Profiler
from celery import group
import copy
profiler = Profiler()
profiler.mark()

varId = 'u'
domainSpec = { 'id': 'merra_u750', 'time': { 'start': '1979-1', 'step': 6 } }
varSpec = {'dset': '/usr/local/web/data/MERRA/u750/merra_u750_1979_1982.nc', 'id': varId }
location0 = {'latitude': -40.0, 'longitude': 50.0}
location1 = {'latitude': -40.0, 'longitude': 70.0}
op = ''
buildDomain = False
nProc = 4

if buildDomain:

    response = group( createDomain.s( iProc, domainSpec ) for iProc in xrange(nProc) )()
    domainId = response.get()[0]
    profiler.mark('createDomain')

    response = group( addVariable.s( domainId, varSpec ) for iProc in xrange(nProc) )()
    variableId = response.get()[0]
    profiler.mark('addVariable')
else:
    domainId = 'merra_u750'
    variableId = 'u'

response = group( computeTimeseries.s( domainId, variableId, location0, op ) for iProc in xrange(nProc) )()
result = response.get()
profiler.mark('timeseries')

response = group( computeTimeseries.s( domainId, variableId, location1, op ) for iProc in xrange(nProc) )()
result = response.get()
profiler.mark('timeseries')

r0 = result[0]
nr0 = len(r0)
profiler.dump( " Received Result, len[0]=%d, sample values[0]=%s, with times:" % ( nr0, str( r0[0:min(5,nr0-1)] ) ) )


