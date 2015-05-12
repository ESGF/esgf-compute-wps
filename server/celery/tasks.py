from celery import Celery
import cdms2, cdutil, sys
from domain import DomainBasedTask
import logging
logger = logging.getLogger('celery.task')

app = Celery( 'tasks', broker='amqp://guest@localhost//', backend='amqp' )

def task_error( msg ):
    logger.error( msg )

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json','pickle'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='pickle',
)

@app.task(base=DomainBasedTask,name='tasks.createDomain')
def createDomain( pIndex, domainSpec ):
    logger.debug( 'app.task: createDomain[%d]: %s ' % (pIndex, str(domainSpec) ))
    logger.debug( 'Task: %s ' % ( app.current_task.__class__.__name__ ))
    return createDomain.createDomain( pIndex, domainSpec )

@app.task(base=DomainBasedTask,name='tasks.removeDomain')
def removeDomain( domainId ):
    removeDomain.removeDomain(domainId)

@app.task(base=DomainBasedTask,name='tasks.addVariable')
def addVariable( domainId, varSpec ):
    logger.debug( 'app.task: addVariable[%s]: %s ' % (domainId, str(varSpec) ))
    d = addVariable.getDomain( domainId )
    if d is not None:
        f=cdms2.open( varSpec['dset'] )
        varId = varSpec['id']
        variable = f[ varId ]
        d.add_variable( varId, variable, **varSpec )
        return varId
    else:
        task_error( "Missing domain '%s'" % ( domainId ) )
        return None

@app.task(base=DomainBasedTask,name='tasks.removeVariable')
def removeVariable( domainId, varId ):
    d = removeVariable.getDomain( domainId )
    d.remove_variable( varId )

@app.task(base=DomainBasedTask,name='tasks.timeseries')
def computeTimeseries( domainId, varId, region, op ):
    d = computeTimeseries.getDomain( domainId )
    if d is not None:
        variable = d.variables.get( varId, None )
        if variable is not None:
            lat, lon = region['latitude'], region['longitude']
            timeseries = variable(latitude=(lat, lat, "cob"), longitude=(lon, lon, "cob"))
            if op == 'average':
                return cdutil.averager( timeseries, axis='t', weights='equal' ).squeeze().tolist()
            else:
                return timeseries.squeeze().tolist()
        else:
             task_error( "Missing variable '%s' in domain '%s'" % (  varId, domainId ) )
    else:
        task_error( "Missing domain '%s'" % ( domainId ) )
        return []

@app.task(base=DomainBasedTask,name='tasks.mergeResults')
def mergeResults( result_list ):
    return result_list


