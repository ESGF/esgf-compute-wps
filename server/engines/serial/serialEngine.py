from modules import Executable
from kernels.manager import kernelMgr


class SerialEngine(Executable):

    def execute( self, run_args ):
        result = kernelMgr.run( run_args )
        return result