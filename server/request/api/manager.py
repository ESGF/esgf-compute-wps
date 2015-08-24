from dialects.cdas import CDASDialect

class APIManager:

    def __init__(self):
        self.dialects = { }
        self.addDialect( CDASDialect() )

    def addDialect( self, dialect_instance ):
        self.dialects[dialect_instance.id] = dialect_instance

    def getDialect( self, request_parameters ):
        dialect_id = request_parameters.get( 'id', 'cdas' )
        return self.dialects.get( dialect_id, None )

apiManager = APIManager()
