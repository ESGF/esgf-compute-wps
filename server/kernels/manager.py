import sys

from modules.utilities import *
from datacache.manager import dataManager, CachedVariable
from datacache.domains import Region
from request.manager import TaskRequest

class KernelManager:

    def getKernelInputs( self, task_request ):
        read_start_time = time.time()
        data = task_request.data[0]
        region = Region( task_request.region )
        operation = task_request.operation
        if isinstance( operation, basestring ): operation = operation.strip()
        read_start_time = time.time()
        use_cache =  task_request['cache']
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

    def run( self, task_request ):
        wpsLog.debug( " $$$ Kernel Manager Execution: request = %s " % str(task_request) )
        start_time = time.time()
        data, region, operation = self.getKernelInputs( task_request )
        kernel = self.getKernel( operation )
        result =  kernel.run( data, region, operation ) if kernel else [ data['result'] ]
        end_time = time.time()
        wpsLog.debug( " $$$ Kernel Execution Complete, total time = %.2f " % (end_time-start_time) )
        return result



    def getKernel( self, operation ):
        if operation:
            op0 = operation[0]
            if op0.get('kernel','base') == 'time':
                from kernels.timeseries_analysis import TimeseriesAnalytics
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
    pre_cache = False

    if pre_cache:
        cache_args =  {   'data': {'id': 'hur', 'collection': 'MERRA/mon/atmos'},  'region': {'level': [100000.0]},  }
        kernelMgr.run( TaskRequest( task=cache_args ) )

    task_args =  { 'region': '{"longitude": -137.09327695888, "latitude": 35.487604770915, "level": 85000}',
                  'data': '{"collection": "MERRA/mon/atmos", "id": "hur"}',
                  'operation': '[  {"kernel": "time", "type": "departures",  "bounds":"np"}, '
                                  '{"kernel":"time", "type":"climatology", "bounds":"annualcycle"} ]'  }

    kernelMgr.run(  TaskRequest( request=task_args ) )
