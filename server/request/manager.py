import sys, json, time
from modules.utilities import wpsLog
from modules import configuration

class TaskManager:

    def __init__(self):
        self.handler = None

    def getJsonResult( self, result_obj ):
        try:
            results = result_obj.task if (result_obj.__class__.__name__ == 'TaskRequest') else result_obj
            result_list = results if isinstance( results, list ) else [ results ]
            for result in result_list:
                for key,value in result.iteritems():
                    if type(value) not in [ dict, list, str, tuple ]: result[key] = str(value)
            result_json = json.dumps( results )
        except Exception, err:
            wpsLog.error( "\n Error parsing JSON response: %s \n result= %s, results=%s" % ( str(err), str(result), str(results) )  )
            result_json = "[]"
        return result_json

    def updateHandler(self):
        if self.handler is None:
            from staging import stagingRegistry
            self.handler = stagingRegistry.getInstance( configuration.CDAS_STAGING  )
            if self.handler is None:
                wpsLog.warning( " Staging method not configured. Running locally on wps server. " )
                self.handler = stagingRegistry.getInstance( 'local' )

    def processRequest( self, request_parameters ):
        self.updateHandler()
        task_request = TaskRequest( request=request_parameters )
        t0 = time.time()
        wpsLog.debug( "---"*50 + "\n $$$ CDAS Process NEW Request[T=%.3f]: %s \n" % ( t0%1000.0, str( request_parameters ) ) + "---"*50 )
        task_request['engine'] = configuration.CDAS_COMPUTE_ENGINE + "Engine"
        result_obj =  self.handler.execute( task_request )
        result_json = self.getJsonResult( result_obj )
        wpsLog.debug( " >>----------------->>>  CDAS Processed Request (total response time: %.3f sec) " %  ( (time.time()-t0) ) )
        return result_json

    def shutdown(self):
        if self.handler is not None:
            shutdown_task_request = TaskRequest( utility='shutdown.all' )
            self.handler.execute( shutdown_task_request )


class TaskRequest:

    def __init__( self, **args  ):
        from request.api.manager import apiManager
        self.task = {}
        request_parameters = args.get( 'request', None )
        if request_parameters:
     #       wpsLog.debug( "---"*50 + "\n $$$ NEW TASK REQUEST: request = %s \n" % str(request_parameters) )
            dialect = apiManager.getDialect( request_parameters )
            self.task = dialect.getTaskRequestData( request_parameters )
        task_parameters = args.get( 'task', None )
        if task_parameters:
            self.task = task_parameters
     #       wpsLog.debug( "---"*50 + "\n $$$ NEW TASK REQUEST: task = %s \n" % str(task_parameters) )
        config_args = self.task.setdefault( 'config', {} )
        cache_val = config_args.setdefault( 'cache', True )
        utility = args.get( 'utility', None )
        if utility: config_args['utility'] = utility

    def __str__(self): return "TR-%s" % str(self.task)

    def getRequestArg( self, id ):
        return self.task.get( id, None )

    @property
    def rid(self):
        return self.task.get( 'rid', None )

    def setRequestId( self, value ):
        self.task['rid'] = value

    @property
    def wid(self):
        return self.task.get( 'wid', None )

    @property
    def data(self):
        from kernels.cda import DatasetContainer
        return DatasetContainer( self.task.get('data', None) )

    @property
    def region(self):
        from datacache.domains import RegionContainer
        return RegionContainer( self.task.get('region', None) )

    @property
    def operations(self):
        from kernels.cda import OperationContainer
        return OperationContainer( self.task.get('operation', None) )

    def isCacheOp(self):
        return ( self.operations.value == None )

    @property
    def configuration(self):
        return self.task.get('config', None)

    def __getitem__(self, key): return self.configuration.get(key, None)

    def __setitem__(self, key, value ): self.configuration[key] = value

    def update( self, args ):
        self.task.update( args )

taskManager = TaskManager()

def getRegion(iR):
    return 'region={"longitude":%f,"latitude":%f,"level":100000,"time":"2010-01-16T12:00:00"}' % ( -100.0+iR*5, -80.0+iR*5 )

def test01():
    request_parms = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
    request_parms['datainputs'] = [u'[region={"longitude":-44.17499999999998,"latitude":28.0645809173584,"level":100000,"time":"2010-01-16T12:00:00"};data={ "MERRA/mon/atmos": [ "v0:hur" ] };operation=["CDTime.departures(v0,slice:t)","CDTime.climatology(v0,slice:t,bounds:annualcycle)"];]']
    response_json = taskManager.processRequest( request_parms )
    responses = json.loads(response_json)
    print "Responses:"
    for iR, result in enumerate(responses):
#        response_data = result['data']
        print result
    print "Done!"

def test02():
    request_parameters = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
    request_parameters['datainputs'] = [u'[region={"longitude":-142.5,"latitude":-15.635426330566418,"level":100000,"time":"2010-01-16T12:00:00"};data={ "MERRA/mon/atmos": [ "v0:hur", "v1:clt" ] };operation=["CDTime.departures(v0,v1,slice:t)","CDTime.climatology(v0,slice:t,bounds:annualcycle)","CDTime.value(v0)"];]']
    response_json = taskManager.processRequest( request_parameters )
    responses = json.loads(response_json)
    print "Responses:"
    for iR, result in enumerate(responses):
#        response_data = result['data']
        print result
    print "Done!"

def test03():
    request_parameters = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }

    print "Starting CACHE"
    request_parameters['datainputs'] = [u'[region={"level":100000};data={"MERRA/mon/atmos":["v0:hur"]};]']
    response_json = taskManager.processRequest( request_parameters )

    for iOP in range(10):
        region = getRegion(iOP)
        print "Starting OP-%d, r= %s" % (iOP, str(region) )
        request_parameters['datainputs'] =[ '[%s;data={ "MERRA/mon/atmos": [ "v0:hur" ] };operation=["CDTime.departures(v0,slice:t)","CDTime.climatology(v0,slice:t,bounds:annualcycle)"];]' % region ]
        response_json = taskManager.processRequest( request_parameters )
        responses = json.loads(response_json)


    print "Done!"

def test04():
    request_parms = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
    request_parms['datainputs'] = [u'[domain=[{"id":"upper","level":{"start":0,"end":1,"system":"indices"}},{"id":"lower","level":{"start":2,"end":3,"system":"indices"}}];variable={ "MERRA/mon/atmos": [ "v0:hur" ] };operation=["CDTime.departures(v0,slice:t)","CDTime.climatology(v0,slice:t,bounds:annualcycle)"];]']
    response_json = taskManager.processRequest( request_parms )
    responses = json.loads(response_json)
    print "Responses:"
    for iR, result in enumerate(responses):
#        response_data = result['data']
        print result
    print "Done!"

if __name__ == "__main__":
    test04()
