from engines.manager import ComputeEngine

class MpiEngine( ComputeEngine ):

    def getCommunicator( self ):
        from communicator import MpiCommunicator
        return  MpiCommunicator()

    @staticmethod
    def getWorkerIntracom():
        from communicator import MpiWorkerIntracom
        return MpiWorkerIntracom




