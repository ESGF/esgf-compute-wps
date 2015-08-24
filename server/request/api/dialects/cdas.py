from . import WPSDialect
import json

class CDASDialect( WPSDialect ):

    def __init__(self):
        WPSDialect.__init__(self,'cdas')

    def getTaskRequestData( self, request_parameters ):
        task_parameters = {}
        inputs_str = request_parameters.get('datainputs',None)
        if inputs_str:
            lines = inputs_str.split(';')
            for line in lines:
                items = line.split('=')
                if len( items ) > 1:
                    parameter = json.loads( items[1] )
                    if isinstance( parameter, dict ): parameter = [ parameter ]
                    task_parameters[ str(items[0]).strip(" []") ]
            del request_parameters[ 'datainputs' ]
        for key in [ 'data', 'region', 'operation' ]:
            parameter = request_parameters.get(key,None)
            if parameter:
                if isinstance(parameter, basestring):
                    parameter = json.loads( parameter )
                if isinstance( parameter, dict ): parameter = [ parameter ]
                task_parameters[ key ] = parameter
                del request_parameters[key]
        for item in request_parameters.items():
            task_parameters[ item[0] ] = item[1]

        return task_parameters
