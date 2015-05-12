from tasks import computeTimeseries, createDomain, addVariable, mergeResults
from utilities import Profiler
from celery import group, chord
import copy
profiler = Profiler()
profiler.mark()

nMonths = 24
nProc = 24
partitionSize = nMonths/nProc

varId = 'u'
domainSpec = { 'id': 'merra_u750', 'time': { 'start': '1979-1', 'step': partitionSize } }
varSpec = {'dset': '/usr/local/web/data/MERRA/u750/merra_u750_1979_1982.nc', 'id': varId }
location0 = {'latitude': -40.0, 'longitude': 50.0}
location1 = {'latitude': -40.0, 'longitude': 70.0}
op = ''
buildDomain = True

if buildDomain:

    task = group( createDomain.s( iProc, domainSpec ) for iProc in xrange(nProc) )
    domainId = task.apply_async().get()[0]
    profiler.mark('createDomain')

    task = group( addVariable.s( domainId, varSpec ) for iProc in xrange(nProc) )
    variableId = task.apply_async().get()[0]
    profiler.mark('addVariable')
else:
    domainId = 'merra_u750'
    variableId = 'u'

task = group( ( computeTimeseries.s( domainId, variableId, location0, op ) for iProc in xrange(nProc) ) )
results = task.apply_async().get()


profiler.mark('timeseries')
r0 = results[0]
nr0 = len(r0)
profiler.dump( " Received Result, len[0]=%d, sample values[0]=%s, with times:" % ( nr0, str( r0[0:min(5,nr0-1)] ) ) )


