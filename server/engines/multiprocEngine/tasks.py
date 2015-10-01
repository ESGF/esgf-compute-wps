from multiprocessing import Process, Pipe
from kernels.manager import KernelManager
from request.manager import TaskRequest
from modules import configuration
from modules.utilities import *
import cPickle, traceback

def worker_exe( wid, comm ):
    kernelMgr = KernelManager( wid )
    while True:
        try:
            task_request_args =  cPickle.loads( comm.recv_bytes() )
            wpsLog.debug( " MULTIPROC[%s] ---> task_request_args: %s " % ( wid, str( task_request_args ) ) )
            results = kernelMgr.run( TaskRequest(task=task_request_args) )
     #       wpsLog.debug( "\n PPT: Worker[%s] sending response-> RID: %s -----\n" % ( wid, results['rid'] ) )
            comm.send_bytes( cPickle.dumps(results) )
        except Exception, err:
            wpsLog.error( " Error executing kernel on Worker[%s] --->  %s:\n %s " % ( wid, str( err ), traceback.format_exc() ) )


class WorkerManager:

    def __init__(self):
        self.workers = {}
        self.startup( configuration.CDAS_NUM_WORKERS )

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

    def broadcast( self, msg, worker_list = None ):
        comms = []
        for wid, ( local_comm, worker_process ) in self.workers.items():
            if ( worker_list is None ) or (wid in worker_list):
                local_comm.send_bytes( cPickle.dumps(msg) )
                comms.append( local_comm )
        return comms

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
            try:
                local_comm.send_bytes( 'exit' )
                local_comm.close()
                worker_process.terminate()
            except: pass
        self.workers = {}

    def getProcessStats(self):
        rv = {}
        for wid, ( comm, process ) in self.workers.items():
            rv[wid] = { 'name':process.name, "pid":process.pid }
        return rv

worker_manager = WorkerManager()
