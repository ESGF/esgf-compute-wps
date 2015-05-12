from tasks import computeTimeseries, createDomain, addVariable, mergeResults
from utilities import Profiler
from celery import group, chord
import copy
profiler = Profiler()
profiler.mark()

nMonths = 32
nProc = 1
partitionSize = nMonths/nProc

varId0 = 'u'
varId1 = 'tas'
domainSpec = { 'id': 'merra_u750', 'time': { 'start': '1979-1', 'step': partitionSize, 'units': 'month' } }
varSpec0 = {'dset': '/usr/local/web/data/MERRA/u750/merra_u750_1979_1982.nc', 'id': varId0 }
varSpec1 = {'dset': '/usr/local/scratch/glpotter/data/TEST_data/tas_Amon_reanalysis_IFS-Cy31r2_197901-201312.nc', 'id': varId1 }
location0 = {'latitude': -40.0, 'longitude': 50.0}
location1 = {'latitude': -40.0, 'longitude': 70.0}
op = ''
buildDomain = True

if buildDomain:

    task = group( createDomain.s( iProc, domainSpec ) for iProc in xrange(nProc) )
    partitionIds = task.apply_async().get()
    profiler.mark('createDomain')

    task = group( addVariable.s( partitionIds[iProc], varSpec0 ) for iProc in xrange(nProc) )
    varPartIds = task.apply_async().get()
    profiler.mark('addVariable')
else:
    domainId = 'merra_u750'
    variableId = 'u'

task = group( ( computeTimeseries.s( domainId, variableId, location0, op ) for iProc in xrange(nProc) ) )
results = task.apply_async().get()

profiler.mark('timeseries')
profiler.dump( " Received Result with times:" )
for ir, r in enumerate(results):
    print " Result[%d], len=%d, sample values: %s" % ( ir, len(r), str( r[0:min(5,len(r)-1)] ) )


