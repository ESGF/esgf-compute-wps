from kernels.manager import KernelManager
from request.manager import TaskRequest
from modules import configuration
from modules.utilities import *
import cPickle, traceback, numpy, sys
from mpi4py import MPI

comm = MPI.Comm.Get_parent()
size = comm.Get_size()
rank = comm.Get_rank()
active = True
wid = "W-%d"%rank
kernelMgr = KernelManager( wid )

while active:
    try:
        task_request_args = comm.bcast( None, root=0)
        wpsLog.debug( " MULTIPROC[%s] ---> task_request_args: %s " % ( wid, str( task_request_args ) ) )
        results = kernelMgr.run( TaskRequest(task=task_request_args) )
        comm.bcast( results, root=rank)
    except Exception, err:
        wpsLog.error( " Error executing kernel on Worker[%s] --->  %s:\n %s " % ( wid, str( err ), traceback.format_exc() ) )

comm.Disconnect()


