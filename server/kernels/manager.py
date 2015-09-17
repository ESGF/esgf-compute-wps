import sys, traceback

from modules.utilities import *
from datacache.manager import dataManager, CachedVariable
from datacache.domains import Region
from request.manager import TaskRequest
executionRecord = ExecutionRecord()

class KernelManager:

    def getKernelInputs( self, operation, request ):
        read_start_time = time.time()
        use_cache =  request['cache']
        region = request.region.value
        cache_type = CachedVariable.getCacheType( use_cache, operation )
        if cache_type == CachedVariable.CACHE_OP:
            dslice = operation.get('slice',None)
            if dslice: region = Region( region, slice=dslice )
        variables = []
        data_specs = []
        if operation is not None:
            input_dataset = []
            input_ids = operation["inputs"]
            for inputID in input_ids:
                input_dataset.append( request.data.getValue( inputID ) )
        else:
            input_dataset = request.data.values

        for data in input_dataset:
            if data is not None:
                variable, data_spec = dataManager.loadVariable( data, region, cache_type )
                cached_region = Region( data_spec['region'] )
                if (region is not None) and (cached_region <> region):
                    subset_args = region.toCDMS()
                    variable = numpy.ma.fix_invalid( variable( **subset_args ) )
        #            wpsLog.debug( " $$$ Subsetting variable: args = %s\n >> in = %s\n >> out = %s " % ( str(subset_args), str(variable.squeeze().tolist()), str(subset_var.squeeze().tolist()) ))
                variables.append( variable )
                data_specs.append( data_spec )
            else:
                raise Exception( "Can't find data matching operation input '%s'", inputID)
        read_end_time = time.time()
        wpsLog.debug( " $$$ DATA READ Complete (domain = %s): %s " % ( str(region), str(read_end_time-read_start_time) ) )
        return variables, data_specs, region

    def persist( self, **args ):
        dataManager.persist( **args )

    def run( self, task_request ):
        response = {}
        response['rid'] = task_request.rid
        results = []
        response['results'] = results
        wpsLog.debug( "---"*50 + "\n $$$ Kernel Manager START NEW TASK: request = %s \n" % str(task_request) )
        start_time = time.time()
        utility = task_request['utility']
        if utility is not None:
            if utility == 'worker.cache':
                response['stats'] = dataManager.stats( rid=task_request.rid )
                wpsLog.debug( "\n *---worker.cache---* Utility request: %s\n" % str( response['stats'] ) )
            else:
                wpsLog.debug( " Unrecognized utility command: %s " % str(utility) )
        else:
            try:
                operations =  task_request.operations
                operations_list = [None] if (operations.value is None) else operations.values
                for operation in operations_list:
                    variables, metadata_recs, region = self.getKernelInputs( operation, task_request )
                    kernel = self.getKernel( operation )
                    result = kernel.run( variables, metadata_recs, region, operation ) if kernel else { 'result': metadata_recs }
                    results.append( result )
                end_time = time.time()
                wpsLog.debug( " $$$ Kernel Execution Complete, ` time = %.2f " % (end_time-start_time) )
            except Exception, err:
                wpsLog.debug( " Error executing kernel: %s " % str(err) )
                wpsLog.debug( traceback.format_exc() )
                response['error'] = str(err)
        return response

    def getKernel( self, operation ):
        if operation:
            if operation.get('kernel','base') == 'time':
                from kernels.timeseries_analysis import TimeseriesAnalytics
                return TimeseriesAnalytics()
            else:
                raise Exception( "No compute kernel found for operations %s" % str(operation) )

kernelMgr = KernelManager()

if __name__ == "__main__":
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)

    from request.manager import TaskRequest
    from manager import kernelMgr
    from modules.configuration import MERRA_TEST_VARIABLES
    import pprint

    pp = pprint.PrettyPrinter(indent=4)
    test_point = [ -137.0, 35.0, 85000 ]
    test_time = '2010-01-16T12:00:00'
    operations = [ "time.departures(v0,t)", "time.climatology(v0,t,annualcycle)", "time.value(v0)" ]

    def getRegion():
        return '{"longitude": %.2f, "latitude": %.2f, "level": %.2f, "time":"%s" }' % (test_point[0],test_point[1],test_point[2],test_time)

    def getData( vars=[0]):
        var_list = ','.join( [ ( '"v%d:%s"' % ( ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '{"%s":[%s]}' % ( MERRA_TEST_VARIABLES["collection"], var_list )
        return data


    def getTaskArgs( op ):
        task_args = { 'region': getRegion(), 'data': getData(), 'operation': json.dumps(op) }
        return task_args

    def test_cache():
        result = kernelMgr.run( TaskRequest( request={ 'region': { "level": 85000 }, 'data': getData() } ) )
        result_stats = result[0]['result']
        pp.pprint(result_stats)

    def test_1():
        result = kernelMgr.run( TaskRequest( request={'config': {'cache': True}, 'region': {'level': 85000}, 'data': '{"name": "hur", "collection": "MERRA/mon/atmos", "id": "v0"}'} ) )
        result_stats = result[0]['result']
        pp.pprint(result_stats)

    def test_departures():
        test_result = [-1.405364990234375, -1.258880615234375, 0.840728759765625, 2.891510009765625, -18.592864990234375, -11.854583740234375, -3.212005615234375, -5.311614990234375, 5.332916259765625, -1.698333740234375]
        task_args = getTaskArgs( op=[operations[ 1 ]] )
        result = kernelMgr.run( TaskRequest( request=task_args ) )
        result_data = result[0]['data']
        pp.pprint(result_data)

    def test_annual_cycle():
        task_args = getTaskArgs( op=[operations[ 2 ]] )
        result = kernelMgr.run( TaskRequest( request=task_args ) )
        result_data = result[0]['data']
        pp.pprint(result_data)

    def test_value_retreval():
        task_args = getTaskArgs( op=[operations[ 3 ]] )
        result = kernelMgr.run( TaskRequest( request=task_args ) )
        result_data = result[0]['data']
        pp.pprint(result_data)


    def test_multitask():
        task_args = getTaskArgs( op=operations )
        results = kernelMgr.run( TaskRequest( request=task_args ) )
        for ir, result in enumerate(results):
            result_data = result['data']
            pp.pprint(result_data)

    def test_api_cache():
        request_parameters = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
        request_parameters['datainputs'] = [u'region={"level":"100000"};data={ "MERRA/mon/atmos":["v0:hur"]}']
        results = kernelMgr.run( TaskRequest( request=request_parameters ) )
        pp.pprint(results)

    def test_api():
        request_parameters = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
        request_parameters['datainputs'] = [u'[region={"longitude":-108.3,"latitude":-23.71042633056642,"level":100000,"time":"2010-01-16T12:00:00"};data={ "MERRA/mon/atmos": [ "v0:hur" ] };operation=["time.departures(v0,slice:t)","time.climatology(v0,slice:t,bounds:annualcycle)","time.value(v0)"]']
        results = kernelMgr.run( TaskRequest( request=request_parameters ) )
        result_data = results[1]['data']
        pp.pprint(result_data)

     # def test_multiproc():
     #    request_parameters = {'embedded': [u'true'], 'service': [u'WPS'], 'rawDataOutput': [u'result'], 'data': {u'MERRA/mon/atmos': [u'v0:hur']}, 'region': {u'latitude': 28.0645809173584, u'level': 100000, u'longitude': -44.17499999999998, u'time': u'2010-01-16T12:00:00'}, 'request': [u'Execute'], 'version': [u'1.0.0'], 'operation': [u'time.departures(v0,slice:t)', u'time.climatology(v0,slice:t,bounds:annualcycle)'], 'identifier': [u'cdas'], 'config': {'cache': True}}
     #    request_parameters['datainputs'] = [u'[region={"longitude":-108.3,"latitude":-23.71042633056642,"level":100000,"time":"2010-01-16T12:00:00"};data={ "MERRA/mon/atmos": [ "v0:hur" ] };operation=["time.departures(v0,slice:t)","time.climatology(v0,slice:t,bounds:annualcycle)","time.value(v0)"]']
     #    results = kernelMgr.run( TaskRequest( request=request_parameters ) )
     #    result_data = results[1]['data']
     #    pp.pprint(result_data)

    test_api()

