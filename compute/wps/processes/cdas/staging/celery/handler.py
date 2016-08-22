from modules.module import Executable
from modules.utilities import wpsLog

class StagingHandler(Executable):

    def execute( self, tesk_request ):
        from staging.celery.manager import submitTask
        wpsLog.info( " Celery staging task, args = '%s' " % ( str( tesk_request ) ) )
        task = submitTask.delay( tesk_request )
        result = task.get()
        return result
