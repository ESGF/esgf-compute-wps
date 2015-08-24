from . import WPSDialect
import json

class CDASDialect( WPSDialect ):

    def __init__(self):
        WPSDialect.__init__(self,'cdas')

    def getTaskRequestData( self, request_parameters ):
        task_parameters = {}
        inputs_str = request_parameters.get('datainputs','')
        if inputs_str:
            lines = inputs_str.split(';')
            for line in lines:
                items = line.split('=')
                if len( items ) > 1:
                    task_parameters[ str(items[0]).strip(" []") ] = json.loads( items[1] )
        return request_parameters, task_parameters