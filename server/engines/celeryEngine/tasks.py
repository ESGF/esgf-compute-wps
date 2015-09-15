from modules.utilities import wpsLog
import celeryconfig
import celery

app = celery.Celery( 'tasks', broker=celeryconfig.BROKER_URL, backend=celeryconfig.CELERY_RESULT_BACKEND )
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json','pickle'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='pickle',
)

def getWorkerName():
    from billiard import current_process
    return current_process().initargs[1]

class DomainBasedTask(celery.Task):
    abstract = True

    def __init__(self):
        celery.Task.__init__(self)

 #    def on_success(self, retval, task_id, args, kwargs):
 #        logger.debug( " !!!!!!!!! Task %s SUCCESSSSSSSSS, rv: %s " % ( task_id, str(retval) ) )
 # #       self.processPendingTask( task_id, retval )

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        wpsLog.error( " Task %s failure, Error: %s " % ( task_id, str(einfo) ) )


from kernels.manager import kernelMgr
from request.manager import TaskRequest

@app.task(base=DomainBasedTask,name='tasks.execute')
def execute( task_request_args ):
    worker = getWorkerName()
    response = kernelMgr.run( TaskRequest(task=task_request_args) )
    response['worker'] = worker
    return response

from engines.manager import ComputeEngine
from communicator import CeleryCommunicator

class CeleryEngine( ComputeEngine ):

    def getCommunicator( self ):
        return  CeleryCommunicator()


