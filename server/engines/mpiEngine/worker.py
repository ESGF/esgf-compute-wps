from kernels.manager import KernelManager
from request.manager import TaskRequest
from modules import configuration
from modules.utilities import *
import traceback
from mpi4py import MPI

comm = MPI.Comm.Get_parent()
size = comm.Get_size()
rank = comm.Get_rank()
active = True
wid = "W-%d"%rank
rid = -1
kernelMgr = KernelManager( wid )

while active:
    status = MPI.Status()
    try:
        task_request_args = comm.recv( source=0, tag=MPI.ANY_TAG, status=status )
        rid = status.Get_tag()
        wpsLog.debug( " MULTIPROC[%s] ---> task_request[%d]: args: %s " % ( wid, rid, str( task_request_args ) ) )
        cfg = task_request_args.get('config','')
        if cfg == "exit": break
        task_request_args['rid'] = rid
        results = kernelMgr.run( TaskRequest(task=task_request_args) )
        comm.send( results, dest=0, tag=rid )
    except Exception, err:
        wpsLog.error( " Error executing task_request[%d] on Worker[%s] --->  %s:\n %s " % ( rid, wid, str( err ), traceback.format_exc() ) )

comm.Disconnect()


