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
        wpsLog.info( "Using compute engine '%s'" % key )
        constructor = self._registry.get( key, None )
        return constructor(key)

class Engine:

    def __init__( self, id ):
        self.id = id

    def execute( self, staging, run_args ):
        if staging == 'local':
            return self.run( run_args )
        elif staging == 'celery':
            from engines.celery.tasks import submitTask
            run_args['engine'] = self.id
            task = submitTask.delay( run_args )
            result = task.get()
            return result
        else:
            raise Exception( "Unrecognized staging configuration: %s" % staging )

    def run(self, run_args ):
        raise Exception( "Attempt to execute virtual Engine base class" )


engineRegistry = ComputeEngineRegistry()




