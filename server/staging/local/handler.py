from staging import StagingMethod
from engines.registry import engineRegistry
from engines.utilities import wpsLog

class StagingHandler(StagingMethod):

    def execute( self, run_args ):
        engine_id = run_args.get( 'engine', None )
        if engine_id is None:
            engine_id = 'serial'
            wpsLog.warning( "Compute Engine not confgured, running serially.")
        engine = engineRegistry.getComputeEngine( engine_id )
        result =  engine.execute( run_args )
        return result