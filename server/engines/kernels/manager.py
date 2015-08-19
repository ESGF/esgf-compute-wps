from modules.utilities import *
from modules import configuration
from datacache.manager import dataManager, CachedVariable
from datacache.domains import Region
import time, sys

class KernelManager:

    def getKernelInputs( self, run_args ):
        read_start_time = time.time()
        data = get_json_arg( 'data', run_args )
        region = Region( run_args.get('region', None ) )
        operation = get_json_arg( 'operation', run_args )
        if isinstance( operation, basestring ): operation = operation.strip()
        read_start_time = time.time()
        use_cache =  get_json_arg( 'cache', run_args, True )
        cache_type = CachedVariable.getCacheType( use_cache, operation )
        variable, result_obj = dataManager.loadVariable( data, region, cache_type )
        cached_region = Region( result_obj['region'] )
        if cached_region <> region:
            variable = numpy.ma.fix_invalid( variable( **region.toCDMS() ) )
        data['variables'] = [ variable ]
        data['result'] = result_obj
        read_end_time = time.time()
        wpsLog.debug( " $$$ DATA READ Complete (domain = %s): %s " % ( str(region), str(read_end_time-read_start_time) ) )
        return data, region, operation

    def run( self, run_args ):
        wpsLog.debug( " $$$ Kernel Manager Execution: run args = %s " % str(run_args) )
        start_time = time.time()
        data, region, operation = self.getKernelInputs( run_args )
        kernel = self.getKernel( operation )
        result =  kernel.run( data, region, operation ) if kernel else [ data['result'] ]
        end_time = time.time()
        wpsLog.debug( " $$$ Kernel Execution Complete, total time = %.2f " % (end_time-start_time) )
        return result



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

if __name__ == "__main__":
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)

    run_args =  {   'data': {'id': 'hur', 'collection': 'MERRA/mon/atmos'},
                    'region': {'level': [100000.0]},
                }
    kernelMgr.run( run_args )

    run_args =  { 'engine': 'celery',
                  'region': '{"longitude": -137.09327695888, "latitude": 35.487604770915, "level": 85000}',
                  'data': '{"collection": "MERRA/mon/atmos", "id": "hur"}',
                  'operation': '[  {"kernel": "time", "type": "departures",  "bounds":"np"}, '
                                  '{"kernel":"time", "type":"climatology", "bounds":"annualcycle"} ]'
                }
    kernelMgr.run( run_args )
