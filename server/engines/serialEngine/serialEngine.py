from modules.module import Executable
from kernels.manager import KernelManager
from request.manager import TaskRequest

kernelMgr = KernelManager( 'W-0' )

class SerialEngine(Executable):

    def execute( self, task_request_args ):
        result = kernelMgr.run( TaskRequest(task=task_request_args) )
        return result