from engines.registry import Engine
from tasks import execute, createDomain
from engines.utilities import *
import json

class CeleryEngine( Engine ):

    def __init__( self, id ):
        Engine.__init__( self, id )
        self.operation = None
        self.data = None
        self.domain = None

    def execute( self, data, region, operation ):
        self.data = json.loads( data )
        self.region = json.loads( region )
        self.operation = json.loads( operation )
        task = execute.delay( data, region, operation )
        result = task.get()
        return result
