import sys, json, time
from staging import stagingRegistry
from modules import configuration
from modules.utilities import wpsLog
from api.manager import apiManager


class TaskManager:

    def __init__(self):
        pass


    def processRequest( self, request_parameters ):
        task_request = TaskRequest( request=request_parameters )
        wpsLog.debug( " $$$ CDAS Process: DataIn='%s', Domain='%s', Operation='%s', ---> Time=%.3f " % ( str( data ), str( region ), str( operation ), time.time() ) )
        t0 = time.time()
        handler = stagingRegistry.getInstance( configuration.CDAS_STAGING  )
        if handler is None:
            wpsLog.warning( " Staging method not configured. Running locally on wps server. " )
            handler = stagingRegistry.getInstance( 'local' )
        result_obj =  handler.execute( task_request, { 'engine': configuration.CDAS_COMPUTE_ENGINE } )
        result_json = json.dumps( result_obj )
        wpsLog.debug( " $$$*** CDAS Process (response time: %.3f sec):\n Result='%s' " %  ( (time.time()-t0), result_json ) )
        return result_json

class TaskRequest:

    def __init__( self, **args  ):
        request_parameters = args.get( 'request', None )
        if request_parameters:
            dialect = apiManager.getDialect( request_parameters )
            self.task = dialect.getTask( request_parameters )
        task_parameters = args.get( 'task', None )
        if task_parameters:
            self.task = task_parameters

    @property
    def data(self):
        dset_mdata = self.task.get('data', None)
        if not isinstance( dset_mdata, (list, tuple) ):  dset_mdata = [ dset_mdata ]
        return dset_mdata

    @property
    def region(self):
        return self.task.get('region', None)

    @property
    def operation(self):
        return self.task.get('operation', None)



taskManager = TaskManager()






