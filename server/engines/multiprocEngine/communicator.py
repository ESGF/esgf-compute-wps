from engines.communicator import ComputeEngineCommunicator, TaskMonitor
from tasks import worker_manager
from modules.utilities import *
import cPickle, time

class MultiprocTaskMonitor(TaskMonitor):

    def __init__( self, rid, **args ):
        TaskMonitor. __init__( self, rid, **args )
        self.comms = args.get( 'comms', [] )
        self.wait_list = list(self.comms)

    def get_response(self, comm ):
        response = cPickle.loads( comm.recv_bytes() )
        return response

    def flush_incoming(self):
        for comm in self.comms:
            while comm.poll():
                response = self.get_response( comm )
                rid = response['rid']
                if rid == self._request_id:
                    self._status = "READY"
                    self.push_response( response )
                    self.wait_list.remove(comm)
                else:
                    task_monitor = self.get_monitor( rid )
                    if task_monitor is not None:
                        task_monitor.push_response( response )

    def response(self, **args):
        self.addStats( **args )
        if len( self.comms ) > 1:
            while len(self.wait_list) > 0:
                self.flush_incoming()
            return self.responses
        elif len( self.comms ) == 1:
            self.flush_incoming()
            if not self.empty():
                response = self.responses.pop()
            else:
                response = self.get_response( self.comms[0] )

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

    def close(self):
        worker_manager.close()

    def submitTaskImpl( self, task_request, worker ):
        rid = self.new_request_id()
        task_request.setRequestId(rid)
        if worker == "*":
            comms = worker_manager.broadcast( task_request.task )
        elif isinstance( worker, list ):
            comms = worker_manager.broadcast( task_request.task, worker  )
        else:
            comms = [ worker_manager.send( task_request.task, worker ) ]
        return MultiprocTaskMonitor( rid, comms=comms )

    def initWorkerStats(self):
       return worker_manager.getProcessStats()



