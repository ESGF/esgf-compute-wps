from . import WPSDialect
from modules.utilities import wpsLog
import json

class CDASDialect( WPSDialect ):

    def __init__(self):
        WPSDialect.__init__(self,'cdas')

    def getTaskRequestData( self, request_params ):
        task_parameters = {}
        request = dict( request_params )
        inputs_str = request.get('datainputs',None)
        wpsLog.debug( ">>>> REQUEST datainputs: %s" % str(inputs_str) )
        if inputs_str:
            lines = inputs_str.split(';')
            for line in lines:
                items = line.split('=')
                if len( items ) > 1:
                    task_parameters[ str(items[0]).strip(" []") ] = json.loads( items[1] )
            del request[ 'datainputs' ]
        for key in [ 'data', 'region', 'operation' ]:
            parameter = request.get(key,None)
            if parameter:
                if isinstance(parameter, basestring):
                    parameter = json.loads( parameter )
                task_parameters[ key ] = parameter
                del request[key]
        for item in request.items():
            task_parameters[ item[0] ] = item[1]

        return task_parameters
