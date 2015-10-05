from modules.module import Executable
from engines import engineRegistry
from modules.utilities import wpsLog

class StagingHandler(Executable):

    def __init__(self, id ):
        Executable.__init__(self, id )
        self.engine = None

    def execute( self, tesk_request ):
        if self.engine is None:
            engine_id = tesk_request[ 'engine' ]
            if engine_id is None:
                engine_id = 'serial'
                wpsLog.warning( "Compute Engine not confgured, running serially.")
            self.engine = engineRegistry.getInstance( engine_id )
        result =  self.engine.execute( tesk_request )
        return result