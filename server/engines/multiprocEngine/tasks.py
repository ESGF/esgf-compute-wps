from engines.manager import ComputeEngine
from communicator import MultiprocCommunicator
from multiprocessing import Process, Pipe
from kernels.manager import kernelMgr
from request.manager import TaskRequest
import cPickle

def worker_process( wid, queue ):
    while True:
        task_request_args = queue.get()
        results = kernelMgr.run( TaskRequest(task=task_request_args) )
        if results: results[0]['worker'] = wid
        queue.put( results )

class WorkerManager:

    def __init__(self):
        self.workers = {}

    def startup( self, nworkers ):
        for iworker in range( nworkers ):
            wid = "W-%d" % iworker
            local_comm, remote_comm = Pipe()
            worker_process = Process(target=worker_process, args=(wid,remote_comm))
            self.workers[wid] = ( local_comm, worker_process )
            worker_process.start()

    def send( self, msg, wid ):
        ( local_comm, worker_process ) = self.workers[wid]
        local_comm.send_bytes( cPickle.dump(msg) )
        return local_comm

    def recv( self, wid ):
        ( local_comm, worker_process ) = self.workers[wid]
        msg = local_comm.recv_bytes()
        return cPickle.loads(msg)

    def shutdown(self):
        for ( local_comm, worker_process ) in self.workers.values():
            local_comm.send_bytes( 'exit' )
            local_comm.close()
            worker_process.terminate()
        self.workers = {}

class MultiprocEngine( ComputeEngine ):

    def getCommunicator( self ):
        return  MultiprocCommunicator()

worker_manager = WorkerManager()
