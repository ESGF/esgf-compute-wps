from engines.communicator import ComputeEngineCommunicator, TaskMonitor
from tasks import worker_manager
import cPickle

class MultiprocTaskMonitor(TaskMonitor):

    def __init__( self, id, **args ):
        TaskMonitor. __init__( self, id, **args )
        self.comm = args.get('comm',None)
        self.stats = {}

    def status(self):
        return self.comm.status()

    def ready(self):
        return self.comm.poll()

    def result( self, **args ):
        self.addStats( **args )
        results = self.recv()
        if len( self.stats ):
            for result in results: result.update( self.stats )
        return results

    def taskName(self):
        return self.id()

    def addStats(self,**args):
        self.stats.update( args )

    def recv( self ):
        msg = self.comm.recv_bytes()
        response = cPickle.loads(msg)
        return response

class MultiprocCommunicator( ComputeEngineCommunicator ):

    def __init__( self ):
        from modules import configuration
        ComputeEngineCommunicator.__init__( self )
        worker_manager.startup( configuration.CDAS_NUM_WORKERS )

    def __del__(self):
        worker_manager.shutdown()

    def submitTask( self, task_request, worker ):
        comm = worker_manager.send( task_request.task, worker )
        return MultiprocTaskMonitor( worker, comm=comm )

    def getWorkerStats(self):
       return worker_manager.getProcessStats()



