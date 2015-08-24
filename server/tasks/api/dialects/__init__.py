from ..manager import apiManager

class WPSDialect:

    def __init__(self,id):
        apiManager.addDialect(id,self)

    def getTaskRequestData( self, request_parameters ):
        pass