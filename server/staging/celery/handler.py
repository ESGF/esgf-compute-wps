from modules import Executable
from modules.utilities import wpsLog

class StagingHandler(Executable):

    def execute( self, tesk_request, run_args ):
        from staging.celery.manager import submitTask
        wpsLog.info( " Celery staging task, args = '%s' " % ( str( run_args ) ) )
        task = submitTask.delay( tesk_request, run_args )
        result = task.get()
        return result
