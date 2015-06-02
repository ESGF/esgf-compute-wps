from base_task import StagingTask
from engines.utilities import wpsLog
from wps import settings
from celery import Celery

app = Celery( 'manager', broker=settings.CDAS_CELERY_BROKER, backend=settings.CDAS_CELERY_BACKEND  )

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json','pickle'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='pickle',
)

@app.task(base=StagingTask,name='manager.submitTask')
def submitTask( run_args ):
    engine_id = run_args['engine']
    engine = submitTask.engines.getComputeEngine( engine_id )
    wpsLog.info( " Celery submit task, args = '%s', engine = %s (%s)" % ( str( run_args ), engine_id, type(engine) ) )
    result =  engine.execute( run_args )
    return result


