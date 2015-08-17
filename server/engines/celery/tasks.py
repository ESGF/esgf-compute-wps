import cdms2
import cdutil

from celery import Celery
import celeryconfig
from base_task import DomainBasedTask
from engines.kernels.manager import kernelMgr
from billiard import current_process
from modules.utilities import *
from celery.utils.log import get_task_logger
logger = get_task_logger('cdas')

def getWorkerName():
    return current_process().initargs[1]

app = Celery( 'tasks', broker=celeryconfig.BROKER_URL, backend=celeryconfig.CELERY_RESULT_BACKEND )

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json','pickle'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='pickle',
)

@app.task(base=DomainBasedTask,name='tasks.execute')
def execute( run_args ):
    worker = getWorkerName()
    results = kernelMgr.run( run_args )
    if results: results[0]['worker'] = worker
    return results

# @app.task(base=DomainBasedTask,name='tasks.mergeResults')
# def mergeResults( result_list ):
#     return result_list
#
@app.task(base=DomainBasedTask,name='tasks.simpleTest')
def simpleTest( input_list ):
     return [ int(v)*3 for v in input_list ]


