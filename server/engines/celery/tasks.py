import cdms2
import cdutil

from celery import Celery
from modules import configuration
from base_task import DomainBasedTask
from engines.kernels.manager import kernelMgr
from modules.utilities import *


app = Celery( 'tasks', broker=configuration.CDAS_CELERY_BROKER, backend=configuration.CDAS_CELERY_BACKEND )

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json','pickle'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='pickle',
)

@app.task(base=DomainBasedTask,name='tasks.createDomain')
def createDomain( pIndex, domainSpec ):
    domainSpec['pIndex'] = pIndex
    wpsLog.debug( 'app.task: createDomain[%d]: %s ' % (pIndex, str(domainSpec) ))
    wpsLog.debug( 'Task: %s ' % ( app.current_task.__class__.__name__ ))
    return createDomain.createDomain( pIndex, domainSpec )

@app.task(base=DomainBasedTask,name='tasks.removeDomain')
def removeDomain( domainId ):
    removeDomain.removeDomain(domainId)

@app.task(base=DomainBasedTask,name='tasks.addVariable')
def addVariable( domainId, varSpec ):
    wpsLog.debug( 'app.task: addVariable[%s]: %s ' % (domainId, str(varSpec) ))
    d = addVariable.getDomain( domainId )
    if d is not None:
        f=cdms2.open( varSpec['dset'] )
        varId = varSpec['id']
        variable = f[ varId ]
        d.add_variable( varId, variable, **varSpec )
        return varId
    else:
        wpsLog.error( "Missing domain '%s'" % ( domainId ) )
        return None

@app.task(base=DomainBasedTask,name='tasks.removeVariable')
def removeVariable( domainId, varId ):
    d = removeVariable.getDomain( domainId )
    d.remove_variable( varId )

@app.task(base=DomainBasedTask,name='tasks.execute')
def execute( run_args ):
    result = kernelMgr.run( run_args )
    return result

@app.task(base=DomainBasedTask,name='tasks.mergeResults')
def mergeResults( result_list ):
    return result_list

@app.task(base=DomainBasedTask,name='tasks.simpleTest')
def simpleTest( input_list ):
    return [ int(v)*3 for v in input_list ]




#
# @app.task(base=DomainBasedTask,name='tasks.timeseries')
# def computeTimeseries( domainId, varId, region, op ):
#     d = computeTimeseries.getDomain( domainId )
#     if d is not None:
#         variable = d.variables.get( varId, None )
#         if variable is not None:
#             lat, lon = region['latitude'], region['longitude']
#             timeseries = variable(latitude=(lat, lat, "cob"), longitude=(lon, lon, "cob"))
#             if op == 'average':
#                 return cdutil.averager( timeseries, axis='t', weights='equal' ).squeeze().tolist()
#             else:
#                 return timeseries.squeeze().tolist()
#         else:
#              wpsLog.error( "Missing variable '%s' in domain '%s'" % (  varId, domainId ) )
#     else:
#         wpsLog.error( "Missing domain '%s'" % ( domainId ) )
#         return []
#
# @app.task(base=DomainBasedTask,name='tasks.execute_d')
# def execute_d( domainId, varId, region, op ):
#     d = computeTimeseries.getDomain( domainId )
#     if d is not None:
#         variable = d.variables.get( varId, None )
#         if variable is not None:
#             lat, lon = region['latitude'], region['longitude']
#             timeseries = variable(latitude=(lat, lat, "cob"), longitude=(lon, lon, "cob"))
#             if op == 'average':
#                 return cdutil.averager( timeseries, axis='t', weights='equal' ).squeeze().tolist()
#             else:
#                 return timeseries.squeeze().tolist()
#         else:
#              wpsLog.error( "Missing variable '%s' in domain '%s'" % (  varId, domainId ) )
#     else:
#         wpsLog.error( "Missing domain '%s'" % ( domainId ) )
#         return []








