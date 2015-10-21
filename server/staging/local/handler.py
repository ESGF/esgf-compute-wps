from modules.module import Executable
from engines import engineRegistry
from modules.utilities import wpsLog
import traceback

class StagingHandler(Executable):

    def __init__(self, id ):
        Executable.__init__(self, id )
        self.engine = None

    def execute( self, tesk_request ):
        try:
            if self.engine is None:
                engine_id = tesk_request[ 'engine' ]
                if engine_id is None:
                    engine_id = 'serial'
                    wpsLog.warning( "Compute Engine not confgured, running serially.")
                self.engine = engineRegistry.getInstance( engine_id )
            wpsLog.debug( "Executing engine '%s', class: %s" % ( self.engine.id, self.engine.__class__.__name__ ) )
            result =  self.engine.execute( tesk_request )
            return result
        except Exception, err:
            wpsLog.debug( "Exception in staging handler: '%s'\n%s" % ( str(err), traceback.format_exc() ) )
            return {}