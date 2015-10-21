import os
import traceback
import importlib
from utilities import wpsLog

class ModuleRegistry:

    def __init__( self, name, directory, package_path ):
        wpsLog.debug( "Init '%s' Module Registry:  dir='%s' package='%s'" % ( name, directory, package_path ) )
        self.name = name
        self._registry = {}
        self._instances = {}
        subdirs = [ o for o in os.listdir(directory) if os.path.isdir(os.path.join(directory,o)) ]

        for modulename in subdirs:
            try:
                module = importlib.import_module( "."+modulename, package_path )
                if hasattr( module, 'getConstructor' ):
                    constructor = module.getConstructor()
                    self._registry[modulename] = constructor
                    wpsLog.debug( " %%---> Registering %s '%s'" % ( self.name, modulename ) )
            except Exception, err:
                wpsLog.warning( "\n --------------- Error Registering %s '%s' --------------- \n %s\n%s" % ( self.name, modulename, str(err), traceback.format_exc() ) )

    def getInstance( self, key  ):
        instance = self._instances.get( key, None )
        if instance is None:
            constructor = self._registry.get( key, None )
            wpsLog.debug( "  Using %s '%s': %s " % ( self.name, key, str(type(constructor)) ) )
            instance = constructor(key)
            self._instances[key] = instance
        return instance

    def getClassInstance( self, key  ):
        constructor = self._registry.get( key, None )
        return constructor



