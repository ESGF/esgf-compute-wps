from engines.manager import ComputeEngine

class MpiEngine( ComputeEngine ):

    def getCommunicator( self ):
        from communicator import MpiCommunicator
        return  MpiCommunicator()






