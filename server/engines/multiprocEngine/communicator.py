from engines.communicator import ComputeEngineCommunicator, TaskMonitor
from tasks import worker_manager

class MultiprocTaskMonitor(TaskMonitor):

    def __init__( self, id, **args ):
        TaskMonitor. __init__( self, id, **args )
        self.comm = args.get('comm',None)
        self.stats = {}

    def status(self):
        return self.task.status

    def ready(self):
        return self.comm.poll()

    def result( self, **args ):
        self.addStats( **args )
        results = self.task.get()
        if len( self.stats ):
            for result in results: result.update( self.stats )
        return results

    def taskName(self):
        return self.task.task_name

    def addStats(self,**args):
        self.stats.update( args )

class MultiprocCommunicator( ComputeEngineCommunicator ):

    def __init__( self ):
        ComputeEngineCommunicator.__init__( self )

    def submitTask( self, task_request, worker ):
        comm = worker_manager.send( (task_request.task,), worker )
        return MultiprocTaskMonitor( worker, comm=comm )

    def getWorkerStats(self):
       pass


