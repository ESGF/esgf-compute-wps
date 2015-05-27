from engines.registry import Engine

class CeleryEngine( Engine ):

    def __init__( self ):
        Engine.__init__(self)

    def execute( self, operation, domain ):
        pass

