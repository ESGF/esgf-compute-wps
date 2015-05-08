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
    CELERY_RESULT_SERIALIZER='json',
)

@app.task(base=DomainBasedTask)
def createDomain( domainSpec ):
    return createDomain.createDomain( domainSpec )

@app.task(base=DomainBasedTask)
def removeDomain( domainId ):
    removeDomain.removeDomain(domainId)

@app.task(base=DomainBasedTask)
def addVariable( domainId, varSpec ):
    d = addVariable.getDomain( domainId )
    if d is not None:
        f=cdms2.open( varSpec['dset'] )
        varId = varSpec['id']
        variable = f( varId )
        d.add_variable( varId, variable )
    else:
        task_error( "Missing domain '%s'" % ( domainId ) )

@app.task(base=DomainBasedTask)
def removeVariable( domainId, varId ):
    d = removeVariable.getDomain( domainId )
    d.remove_variable( varId )

@app.task(base=DomainBasedTask)
def timeseries( domainId, varId, region, op ):
    d = timeseries.getDomain( domainId )
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


