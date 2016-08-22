class Module:

    def __init__( self, id ):
        self.id = id


class Executable(Module):

    def execute( self, run_args ):
        raise Exception( "Attempt to execute virtual Executable base class" )


