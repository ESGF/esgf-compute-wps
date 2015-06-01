
class StagingMethod:

    def __init__( self, id ):
        self.id = id

    def execute( self, run_args ):
        raise Exception( "Attempt to execute virtual StagingMethod base class" )

