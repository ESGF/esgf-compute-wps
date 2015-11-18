import json, sys
from modules.utilities import wpsLog, convert_json_str

class WPSDialect:

    def __init__(self,id):
        self.id = id
        self.dataInputKeys = [ 'data', 'region', 'operation', 'domain', 'variable' ]
        self.controlKeys = [ 'embedded', 'async' ]
        self.aliases = { 'variable':'data', 'domain':'region' }

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
        for key in self.dataInputKeys:
            parameter = request.get(key,None)
            if parameter:
                if isinstance(parameter, basestring):
                    try:
                        parameter = json.loads( parameter )
                    except:
                        wpsLog.error( " Error json decoding parameter: '%s' " % parameter )
                        parameter = ""
                task_parameters[ key ] = parameter
                del request[key]
        for key in self.controlKeys:
            parameter = request.get(key,None)
            if parameter:
                if isinstance(parameter, basestring): parameter = parameter.lower()
                task_parameters[ key ] = parameter
                del request[key]
        for item in request.items():
            task_parameters[ item[0] ] = item[1]

        for key_alias, alt_key in self.aliases.items():
            if key_alias in task_parameters:
                task_parameters[ alt_key ] = task_parameters[ key_alias ]
                del task_parameters[ key_alias ]

        return task_parameters


    def parseDatainputsStr(self, inputs, task_parameters ):
        lines = inputs.split(';')
        for line in lines:
            items = line.split('=')
            if len( items ) > 1:
                try:
                    task_parameters[ str(items[0]).strip(" []") ] = json.loads( items[1] )
                except Exception, err:
                    wpsLog.error( "Error parsing jason string '%s': %s" % ( items[1], str(err) ) )
                    print>>sys.stderr, "Error parsing jason string '%s': %s" % ( items[1], str(err) )


if __name__ == "__main__":

    requestData = { 'datainputs': [u'[domain={"longitude":-122.55,"latitude":-33.25,"level":100000,"time":"2010-01-16T12:00:00"};variable={ "MERRA/mon/atmos": [ "v0:hur" ] };operation=["CDTime.departures(v0,slice:t)","CDTime.climatology(v0,slice:t,bounds:annualcycle)","CDTime.value(v0)"];]'] }
    dialect = WPSDialect('cdas')
    result = dialect.getTaskRequestData( requestData )
    print result


