import logging, os, importlib, json, traceback
from engines.utilities import *

class ComputeEngineRegistry:

    def __init__( self ):
        self._registry = {}
        dir = os.path.dirname(__file__)
        subdirs = [ o for o in os.listdir(dir) if os.path.isdir(os.path.join(dir,o)) ]

        for modulename in subdirs:
            try:
                module = importlib.import_module("."+modulename,'engines')
                if hasattr( module, 'getConstructor' ):
                    constructor = module.getConstructor()
                    self._registry[modulename] = constructor
                    wpsLog.debug( "Registering compute engine '%s'" % modulename )
            except Exception:
                wpsLog.warning( "\n --------------- Error Registering compute engine '%s' --------------- \n %s" % ( modulename, traceback.format_exc() ) )

    def getComputeEngine( self, key  ):
        wpsLog.info( "  @@@@  key = %s " % key )
        constructor = self._registry.get( key, None )
        wpsLog.info( "  %%%% Using compute engine '%s': %s " % ( key, str(type(constructor)) ) )
        return constructor(key)

class Engine:

    def __init__( self, id ):
        from staging.registry import stagingRegistry
        self.id = id
        self.handlers = stagingRegistry

    def execute( self, run_args ):
        raise Exception( "Executing engine virtual base class" )


engineRegistry = ComputeEngineRegistry()




