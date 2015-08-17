from modules.utilities import *
from modules import configuration
from datacache.manager import dataManager, CachedVariable
from datacache.domains import Region
import time

class KernelManager:

    def getKernelInputs( self, run_args ):
        read_start_time = time.time()
        data = get_json_arg( 'data', run_args )
        region = Region( run_args.get('region', None ) )
        operation = get_json_arg( 'operation', run_args )
        read_start_time = time.time()
        use_cache =  get_json_arg( 'cache', run_args, True )
        cache_type = CachedVariable.getCacheType( use_cache, operation )
        variable, result_obj = dataManager.loadVariable( data, region, cache_type )
        cached_region = Region( result_obj['region'] )
        if cached_region <> region:
            variable = numpy.ma.fix_invalid( variable( **region.getCDMS() ) )
        data['variables'] = [ variable ]
        data['result'] = result_obj
        read_end_time = time.time()
        wpsLog.debug( " $$$ DATA READ Complete (domain = %s): %s " % ( str(region), str(read_end_time-read_start_time) ) )
        return data, region, operation

    def run( self, run_args ):
        data, region, operation = self.getKernelInputs( run_args )
        kernel = self.getKernel( operation )
        if kernel:
            return kernel.run( data, region, operation )
        else:
            return [ data['result'] ]

    def getKernel( self, operation ):
        if operation:
            op0 = operation[0]
            if op0.get('kernel','base') == 'time':
                from timeseries_analysis import TimeseriesAnalytics
                return TimeseriesAnalytics()
            else:
                raise Exception( "No compute kernel found for operations %s" % str(operation) )

    def processOperations( self, operations_spec ):
        op_spec_list = operations_spec.split(';')
        for op_spec in op_spec_list:
            op_spec_toks = op_spec.split('=')
            if len( op_spec_toks ) == 2:
                dataManager.addCachedVariable( op_spec_toks[0].strip(), op_spec_toks[1].strip() )
            else:
                pass # TODO: utility cmds

        result_variables = self.data_cache.getResults()
        results = {}
        for result_variable in result_variables:
            kernel = self.getKernel( result_variable.operation )
            if kernel:
                result = kernel.run( result_variable.operation )
                results[ result_variable.id ] = result
            else:
                raise Exception( "No compute kernel found for operation %s" % str( result_variable.operation ) )
        return results





kernelMgr = KernelManager()
