import logging
logger = logging.getLogger('celery.task')
from celery import Task

class DomainBasedTask(Task):
    abstract = True
    PendingTasks = {}

    def __init__(self):
        Task.__init__(self)

    def on_success(self, retval, task_id, args, kwargs):
        logger.debug( " !!!!!!!!! Task %s SUCCESSSSSSSSS, rv: %s " % ( task_id, str(retval) ) )
 #       self.processPendingTask( task_id, retval )

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error( " Task %s failure, Error: %s " % ( task_id, str(einfo) ) )

    @classmethod
    def processPendingTask( cls, task_id, retval ):
        if task_id == "tasks.execute":
            worker = retval['worker']
            cache_request,cached_domain = cls.PendingTask[ task_id ]
            result = cache_request.get()
            cached_domain.cacheRequestComplete( result['worker'] )
            del cls.PendingTask[ task_id ]
