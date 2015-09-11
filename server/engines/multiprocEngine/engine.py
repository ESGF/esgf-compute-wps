from engines.manager import ComputeEngine

class MultiprocEngine( ComputeEngine ):

    def getCommunicator( self ):
        from communicator import MultiprocCommunicator
        return  MultiprocCommunicator()


