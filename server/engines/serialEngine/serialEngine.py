from modules.module import Executable
from kernels.manager import kernelMgr
from request.manager import TaskRequest


class SerialEngine(Executable):

    def execute( self, task_request_args ):
        result = kernelMgr.run( TaskRequest(task=task_request_args) )
        return result