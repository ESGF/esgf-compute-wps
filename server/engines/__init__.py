from modules.registry import ModuleRegistry
from modules.utilities import wpsLog
import os

directory = os.path.dirname(__file__)
package = os.path.basename( directory )

class EngineRegistry(ModuleRegistry):

    def __init__( self, name, directory, package_path, **args ):
        from modules.configuration import CDAS_COMPUTE_ENGINE
        current_compute_engine = CDAS_COMPUTE_ENGINE + "Engine"
        ModuleRegistry.__init__( self, name, directory, package_path, current_module=current_compute_engine, **args )

    def getWorkerIntracom( self, key  ):
        class_instance = self._registry.get( key, None )
        intracom = class_instance.getWorkerIntracom()
#         wpsLog.debug( "\n\n getWorkerIntracom::------->  Using %s '%s': %s, %s \n\n" % ( self.name, key, str(class_instance), str(intracom) ) )
        return intracom

engineRegistry = EngineRegistry( 'Compute Engine', directory, package )
