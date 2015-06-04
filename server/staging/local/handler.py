from modules import Executable
from engines import engineRegistry
from modules.utilities import wpsLog

class StagingHandler(Executable):

    def execute( self, run_args ):
        engine_id = run_args.get( 'engine', None )
        if engine_id is None:
            engine_id = 'serial'
            wpsLog.warning( "Compute Engine not confgured, running serially.")
        engine = engineRegistry.getInstance( engine_id )
        result =  engine.execute( run_args )
        return result