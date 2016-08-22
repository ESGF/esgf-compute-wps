from base_task import StagingTask
from modules.utilities import wpsLog
from engines.celery import celeryconfig
from modules import configuration
from celery import Celery

app = Celery( 'manager', broker=celeryconfig.BROKER_URL, backend=celeryconfig.CELERY_RESULT_BACKEND )

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json','pickle'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='pickle',
)

@app.task(base=StagingTask,name='manager.submitTask')
def submitTask( tesk_request ):
    engine_id = tesk_request['engine']
    engine = submitTask.engines.getInstance( engine_id )
    wpsLog.info( " Celery submit task, args = '%s', engine = %s (%s)" % ( str( tesk_request ), engine_id, type(engine) ) )
    result =  engine.execute( tesk_request )
    return result


