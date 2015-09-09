from request.api.dialects import WPSDialect
from modules.utilities import wpsLog, convert_json_str
import json

class CDASDialect( WPSDialect ):

    def __init__(self):
        WPSDialect.__init__(self,'cdas')

    def getTaskRequestData( self, request_params ):
        task_parameters = {}
        request = dict( request_params )
        inputs = request.get('datainputs',None)
        wpsLog.debug( ">>>> REQUEST datainputs: %s" % str(inputs) )
        if inputs:
            if isinstance( inputs, dict ):
                task_parameters.update( inputs )
            elif isinstance( inputs, list ):
                for item in inputs:
                    if isinstance( item, list ):
                        task_parameters[ str(item[0]) ] = convert_json_str( item[1] )
                    elif isinstance( item, basestring ):
                        self.parseDatainputsStr( item, task_parameters )
            else:
                self.parseDatainputsStr(self, inputs, task_parameters )
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


    def parseDatainputsStr(self, inputs, task_parameters ):
        lines = inputs.split(';')
        for line in lines:
            items = line.split('=')
            if len( items ) > 1:
                task_parameters[ str(items[0]).strip(" []") ] = json.loads( items[1] )


if __name__ == "__main__":

    cd = CDASDialect()
    request = { 'datainputs': [u'[region={"level":"100000"};data={"collection":"MERRA/mon/atmos","id":"hur"};]'] }
    result = cd.getTaskRequestData( request )
    print result