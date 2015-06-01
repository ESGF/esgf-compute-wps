import logging, os, importlib, json, traceback
from engines.utilities import *

class StagingMethodRegistry:

    def __init__( self ):
        self._registry = {}
        dir = os.path.dirname(__file__)
        subdirs = [ o for o in os.listdir(dir) if os.path.isdir(os.path.join(dir,o)) ]

        for modulename in subdirs:
            try:
                module = importlib.import_module("."+modulename,'staging')
                if hasattr( module, 'getConstructor' ):
                    constructor = module.getConstructor()
                    self._registry[modulename] = constructor
                    wpsLog.debug( "Registering staging method '%s'" % modulename )
            except Exception:
                wpsLog.warning( "\n --------------- Error Registering staging method '%s' --------------- \n %s" % ( modulename, traceback.format_exc() ) )

    def getHandler( self, key  ):
        constructor = self._registry.get( key, None )
        if constructor is not None:
            wpsLog.info( "Using staging method '%s'" % key )
        return constructor(key)



stagingRegistry = StagingMethodRegistry()





