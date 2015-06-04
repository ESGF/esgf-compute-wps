from modules import Executable
from tasks import execute
from modules.utilities import *

class CeleryEngine( Executable ):

    def execute( self, run_args ):
        wpsLog.info( "Executing Celery engine, args: %s" % ( run_args ) )
        task = execute.delay( run_args )
        result = task.get()
        return result
