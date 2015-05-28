from engines.registry import Engine
from tasks import submitTask, createDomain
import json

class CeleryEngine( Engine ):

    def __init__(self):
        Engine.__init__(self)
        self.operation = None
        self.data = None
        self.domain = None

    def execute( self, data, region, operation ):
        self.data = json.loads( data )
        self.region = json.loads( region )
        self.operation = json.loads( operation )
        task = submitTask.delay( data, region, operation )
        result = task.get()
        return result
