from engines.communicator import ComputeEngineCommunicator, TaskMonitor
from tasks import worker_manager
from Queue import LifoQueue
import cPickle

class MultiprocTaskMonitor(TaskMonitor):

    def __init__( self, rid, **args ):
        TaskMonitor. __init__( self, rid, **args )
        self.comm = args.get('comm',None)
        self.stats = {}
        self.responses = LifoQueue()

    def push_response(self,response):
        self.responses.put_nowait( response )

    def status(self):
        return self.comm.status()

    def ready(self):
        self.flush_incoming()
        return not self.responses.empty()

    def flush_incoming(self):
        while self.comm.poll():
            response = self.recv()
            rid = response['rid']
            if rid == self._rid:
                self.push_response( response )
            else:
                task_monitor = self.get_monitor( rid )
                task_monitor.push_response( response )

    def response(self, **args):
        self.addStats( **args )
        self.flush_incoming()
        if not self.responses.empty():
            response = self.responses.get()
        else:
            response = self.recv()
        return response

    def result( self, **args ):
        response = self.response( **args )
        results = response['results']
        if len( self.stats ):
            for result in results: result.update( self.stats )
        return results

    def taskName(self):
        return self.rid

    def addStats(self,**args):
        self.stats.update( args )

    def recv( self ):
        msg = self.comm.recv_bytes()
        response = cPickle.loads(msg)
        return response

class MultiprocCommunicator( ComputeEngineCommunicator ):

    RequestIndex = 0

    @classmethod
    def new_request_id(cls):
        cls.RequestIndex = cls.RequestIndex + 1
        return 'T-%d' % cls.RequestIndex

    def __init__( self ):
        from modules import configuration
        ComputeEngineCommunicator.__init__( self )

    def submitTaskImpl( self, task_request, worker ):
        rid = self.new_request_id()
        task_request.rid = rid
        comm = worker_manager.send( task_request.task, worker )
        return MultiprocTaskMonitor( rid, comm=comm )

    def getWorkerStats(self):
       return worker_manager.getProcessStats()



