from engines.registry import Engine
from tasks import execute, createDomain
from engines.utilities import *

class CeleryEngine( Engine ):

    def execute( self, run_args ):
        wpsLog.info( "Executing Celery engine, args: %s" % ( run_args ) )
        task = execute.delay( run_args )
        result = task.get()
        return result
