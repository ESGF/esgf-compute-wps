from engines.utilities import get_json_arg

class KernelManager:

    def __init__( self ):
        pass

    def run( self, run_args ):
        operation = get_json_arg( 'operation', run_args )
        kernel = self.getKernel( operation )
        if kernel:
            result = kernel.run( run_args )
            return result
        else:
            raise Exception( "No compute kernel found for operation %s" % str(operation) )

    def getKernel( self, operation ):
        from timeseries_analysis import TimeseriesAnalytics
        return TimeseriesAnalytics( operation )


kernelMgr = KernelManager()
