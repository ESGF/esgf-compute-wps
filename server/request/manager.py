import sys, json, time
from modules.utilities import wpsLog

class TaskManager:

    def __init__(self):
        pass

    def processRequest( self, request_parameters ):
        from modules import configuration
        from staging import stagingRegistry
        task_request = TaskRequest( request=request_parameters )
        wpsLog.debug( "---"*50 + "\n $$$ CDAS Process NEW Request[T=%.3f]: %s \n" % ( time.time(), str( request_parameters ) ) + "---"*50 )
        t0 = time.time()
        handler = stagingRegistry.getInstance( configuration.CDAS_STAGING  )
        if handler is None:
            wpsLog.warning( " Staging method not configured. Running locally on wps server. " )
            handler = stagingRegistry.getInstance( 'local' )
        task_request['engine'] = configuration.CDAS_COMPUTE_ENGINE
        result_obj =  handler.execute( task_request )
        result_json = json.dumps( result_obj )
        wpsLog.debug( " $$$*** CDAS Processed Request (total response time: %.3f sec) " %  ( (time.time()-t0) ) )
        return result_json

class TaskRequest:

    def __init__( self, **args  ):
        from request.api.manager import apiManager
        self.task = {}
        request_parameters = args.get( 'request', None )
        if request_parameters:
            dialect = apiManager.getDialect( request_parameters )
            self.task = dialect.getTaskRequestData( request_parameters )
        task_parameters = args.get( 'task', None )
        if task_parameters:
            self.task = task_parameters
        self.task['config'] = { 'cache' : True }

    def __str__(self): return "TR-%s" % str(self.task)

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

    @property
    def configuration(self):
        return self.task.get('config', None)

    def __getitem__(self, key): return self.configuration.get(key, None)

    def __setitem__(self, key, value ): self.configuration[key] = value

taskManager = TaskManager()




