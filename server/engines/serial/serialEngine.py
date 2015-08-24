from modules import Executable
from kernels.manager import kernelMgr


class SerialEngine(Executable):

    def execute( self, task_request, run_args ):
        result = kernelMgr.run( task_request, run_args )
        return result