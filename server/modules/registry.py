import os
import traceback
import importlib
from utilities import wpsLog

class ModuleRegistry:

    def __init__( self, name, directory, package_path, **args ):
     #   wpsLog.debug( "Init '%s' Module Registry:  dir='%s' package='%s'" % ( name, directory, package_path ) )
        self.name = name
        self.current_module = args.get('current_module',None)
        self._registry = {}
        self._instances = {}
        subdirs = [ o for o in os.listdir(directory) if os.path.isdir(os.path.join(directory,o)) ]

        for modulename in subdirs:
            if (self.current_module is None) or (self.current_module == modulename):
                try:
                    module = importlib.import_module( "."+modulename, package_path )
                    if hasattr( module, 'getConstructor' ):
                        constructor = module.getConstructor()
                        self._registry[modulename] = constructor
     #                   wpsLog.debug( " %%---> Registering %s '%s'" % ( self.name, modulename ) )
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

    def call_dbg(self, msg):
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()
        wpsLog.debug( "\n\n -------------< %s >-------------- MPI proc %d of %d (%x):" % ( msg, rank, size, os.getpid() ) )
        wpsLog.debug( repr(traceback.format_stack()) + "\n\n" )



