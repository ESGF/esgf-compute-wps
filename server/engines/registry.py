import logging, os, importlib
wpsLog = logging.getLogger('wps')
wpsLog.setLevel(logging.DEBUG)
if len( wpsLog.handlers ) == 0:
    wpsLog.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log' ) )))

class ComputeEngineRegistry:

    def __init__( self ):
        self._registry = {}
        dir = os.path.dirname(__file__)
        subdirs = [ o for o in os.listdir(dir) if os.path.isdir(os.path.join(dir,o)) ]
        wpsLog.info( "Registering compute engines from list: %s" % str(subdirs) )

        for modulename in subdirs:
            try:
                module = importlib.import_module("."+modulename,'engines')
                constructor = module.getConstructor()
                self._registry[modulename] = constructor
                wpsLog.info( "Registering compute engine '%s'" % modulename )
            except Exception, err:
                wpsLog.warning( "Error Registering compute engine '%s': %s" % ( modulename, str(err) ) )

    def getComputeEngine( self, key  ):
        wpsLog.info( "Using compute engine '%s'" % key )
        constructor = self._registry.get( key, None )
        return constructor()

class Engine:

    def __init__( self ):
        pass

    def execute( self, data, region, operation ):
        pass


engineRegistry = ComputeEngineRegistry()




