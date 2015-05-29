from engines.registry import Engine
from engines.kernels.manager import kernelMgr
import os, logging
from engines.utilities import wpsLog

class SerialEngine(Engine):

    def run( self, run_args ):
        result = kernelMgr.run( run_args )
        return result