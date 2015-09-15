from multiprocessing import Process, Pipe
from kernels.manager import kernelMgr
from request.manager import TaskRequest
from modules.utilities import *
import cPickle

def worker_exe( wid, comm ):
    while True:
        task_request_args =  cPickle.loads( comm.recv_bytes() )
        results = kernelMgr.run( TaskRequest(task=task_request_args) )
        if results: results['worker'] = wid
        comm.send_bytes( cPickle.dumps(results) )

class WorkerManager:

    def __init__(self):
        self.workers = {}

    def  __del__(self):
        self.shutdown()

    def startup( self, nworkers ):
        if len( self.workers ) == 0:
            for iworker in range( nworkers ):
                wid = "W-%d" % iworker
                local_comm, remote_comm = Pipe()
                worker_process = Process(target=worker_exe, name=wid, args=(wid,remote_comm))
                self.workers[wid] = ( local_comm, worker_process )
                worker_process.start()
                wpsLog.debug( "Started worker %s: process %s[%s]" % ( wid, worker_process.name, str(worker_process.pid) ) )

    def send( self, msg, wid ):
        ( local_comm, worker_process ) = self.workers[wid]
        local_comm.send_bytes( cPickle.dumps(msg) )
        return local_comm

    def recv( self, wid ):
        ( local_comm, worker_process ) = self.workers[wid]
        msg = local_comm.recv_bytes()
        response = cPickle.loads(msg)
        wpsLog.debug( "WorkerManager--> receiving response from [%s]: %s" % ( wid, str(response) ) )

    def shutdown(self):
        for ( local_comm, worker_process ) in self.workers.values():
            local_comm.send_bytes( 'exit' )
            local_comm.close()
            worker_process.terminate()
        self.workers = {}

    def getProcessStats(self):
        rv = {}
        for wid, ( comm, process ) in self.workers.items():
            rv[wid] = { 'name':process.name, "pid":process.pid }
        return rv

worker_manager = WorkerManager()
