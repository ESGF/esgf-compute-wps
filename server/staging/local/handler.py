from modules.module import Executable
from engines import engineRegistry
from modules.utilities import wpsLog

class StagingHandler(Executable):

    def execute( self, tesk_request ):
        engine_id = tesk_request[ 'engine' ]
        if engine_id is None:
            engine_id = 'serial'
            wpsLog.warning( "Compute Engine not confgured, running serially.")
        engine = engineRegistry.getInstance( engine_id )
        result =  engine.execute( tesk_request )
        return result