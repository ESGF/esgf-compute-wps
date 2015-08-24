
class APIManager:

    def __init__(self):
        self.dialects = { }

    def addDialect( self, id, constructor ):
        self.dialects[id] = constructor


    def getDialect( self, request_parameters ):
        dialect_id = request_parameters.get( 'id', None )
        if dialect_id:
            constructor = self.dialects.get( dialect_id, None )
            return constructor() if constructor else None


apiManager = APIManager()
