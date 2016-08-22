from engines.manager import ComputeEngine
from modules.utilities import wpsLog

class MpiEngine( ComputeEngine ):

    def getCommunicator( self ):
        from communicator import MpiCommunicator
        return  MpiCommunicator()

    @classmethod
    def getWorkerIntracom(cls):
        from communicator import MpiWorkerIntracom
        rv = MpiWorkerIntracom()
        return rv





