from modules import Executable
from modules.utilities import wpsLog

class StagingHandler(Executable):

    def execute( self, run_args ):
        from staging.celery.manager import submitTask
        wpsLog.info( " Celery staging task, args = '%s' " % ( str( run_args ) ) )
        task = submitTask.delay( run_args )
        result = task.get()
        return result
