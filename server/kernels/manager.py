import sys
import traceback

from modules.utilities import *
from datacache.manager import DataManager, CachedVariable
from datacache.domains import Region
executionRecord = ExecutionRecord()

class KernelManager:

    def __init__(self, wid ):
        self.dataManager = DataManager( wid )

    def persistStats( self, **args ):
        self.dataManager.persistStats( **args )

    def getKernelInputs( self, operation, op_index, request ):
        read_start_time = time.time()
        use_cache =  request['cache']
        embedded = request.getBoolRequestArg('embedded')
        result_names = request['result_names']
        regions = request.region
        cache_type = CachedVariable.getCacheType( use_cache, operation )
        variables = []
        data_specs = []
        if operation is not None:
            input_dataset = []
            input_ids = operation["inputs"]
            for inputID in input_ids:
                if inputID[0] == "$":
                    try:
                        inputIndex = int(inputID[1:])
                        input_dataset.append( request.data.values[inputIndex] )
                    except Exception, err:
                        raise Exception( "Input ID '%s' syntax error: %s" % ( inputID, str(err) ) )
                elif inputID == "*":
                    for input_dset in request.data.values:
                        input_dataset.append( input_dset )
                else:
                    input_dataset.append( request.data.getValue( inputID ) )
        else:
            input_dataset = request.data.values

        for data in input_dataset:
            if data is not None:
                region_id = data['domain']
                region = regions.getValue( region_id, True )
                if cache_type == CachedVariable.CACHE_OP:
                    dslice = operation.get('slice',None)
                    if dslice: region = Region( region, slice=dslice )
                variable, data_spec = self.dataManager.loadVariable( data, region, cache_type )
                data_spec['missing'] = variable.getMissing()
                cached_region = Region( data_spec['region'] )
                if (region is not None) and (cached_region <> region):
                    subset_args = region.toCDMS()
                    variable = numpy.ma.fix_invalid( variable( **subset_args ) )
        #            wpsLog.debug( " $$$ Subsetting variable: args = %s\n >> in = %s\n >> out = %s " % ( str(subset_args), str(variable.squeeze().tolist()), str(subset_var.squeeze().tolist()) ))
                data_spec['data.region'] = region
                data_spec['embedded'] = embedded
                if result_names is not None: data_spec['result_name'] = result_names[ op_index ]
                variables.append( variable )
                data_specs.append( data_spec )
            else:
                raise Exception( "Can't find data matching operation input '%s'", inputID)
        read_end_time = time.time()
        wpsLog.debug( " $$$ DATA READ Complete: %.2f, data_specs: %s " % ( (read_end_time-read_start_time), str(data_specs) ) )
        return variables, data_specs

    def persist( self, **args ):
        self.dataManager.persist( **args )

    def run( self, task_request ):
        response = {}
        response['rid'] = task_request.rid
        response['wid'] = self.dataManager.getName()
        results = []
        response['results'] = results
        start_time = time.time()
        utility = task_request['utility']
        if utility is not None:
            if utility == 'worker.cache':
                response['stats'] = self.dataManager.stats()
            elif utility == 'shutdown.all':
                self.shutdown()
            elif utility == 'domain.transfer':
                self.dataManager.transferDomain( task_request.getRequestArg('source'), task_request.getRequestArg('destination'),  task_request.getRequestArg('region'),  task_request.getRequestArg('var') )
            elif utility=='domain.uncache':
                self.dataManager.uncache( task_request.data.values, task_request.region.value )
                response['stats'] = self.dataManager.stats()
            else:
                wpsLog.debug( " Unrecognized utility command: %s " % str(utility) )
        else:
            try:
                wpsLog.debug( "---"*50 + "\n $$$ Kernel Manager START NEW TASK[%.4f]: request = %s \n" % ( time.time()%1000.0, str(task_request) ) )
                operations =  task_request.operations
                operations_list = [None] if (operations.value is None) else operations.values
                for op_index, operation in enumerate(operations_list):
                    variables, metadata_recs = self.getKernelInputs( operation, op_index, task_request )
                    kernel = self.getKernel( operation )
                    result = kernel.run( variables, metadata_recs, operation ) if kernel else { 'result': metadata_recs }
                    results.append( result )
                end_time = time.time()
                wpsLog.debug( " $$$ Kernel Execution Complete, ` time = %.2f " % (end_time-start_time) )
            except Exception, err:
                wpsLog.debug( " Error executing kernel: %s " % str(err) )
                wpsLog.debug( traceback.format_exc() )
                response['error'] = str(err)

        genericize( results )
        self.persistStats( loc='KM-exit', wid=self.dataManager.getName() )
        return response


    def shutdown(self):
        wpsLog.debug( "PERSIST (@shutdown) data and metadata " )
        self.persist( loc='shutdown', wid=self.dataManager.getName() )
        self.dataManager.close()
        sys.exit(0)

    def getKernel( self, operation ):
        from kernels import kernelRegistry
        if operation:
            kernel_id = operation.get('kernel','base')
            kernel = kernelRegistry.getInstance( kernel_id )
            if kernel == None:
                raise Exception( "No compute kernel found for operations %s" % str(operation) )
            return kernel

if __name__ == "__main__":
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)

    from request.manager import TaskRequest
    from request.api.dialects import WPSDialect
    from modules.configuration import MERRA_TEST_VARIABLES
    import pprint
    dialect = WPSDialect('cdas')

    kernelMgr = KernelManager('W-3')
    pp = pprint.PrettyPrinter(indent=4)
    test_point = [ -137.0, 35.0, 85000 ]
    test_point1 = [ 137.0, -35.0, 100000 ]
    test_time = '2010-01-16T12:00:00'
    operations = [ "CDTime.departures(v0,slice:t)", "CDTime.climatology(v0,slice:t,bounds:annualcycle)", "CDTime.value(v0)" ]
    ave_operations = [ "CWT.average(v0,axis:z)", "CWT.average(v0,axis:ze)" ]

    def getRegion():
        return '{"longitude": %.2f,"latitude": %.2f, "level": %.2f, "time":"%s","id":"r0"}' % (test_point[0],test_point[1],test_point[2],test_time)


    def getCacheRegion():
        return '{"level": %.2f,"id":"r0"}' % (test_point[2])

    def getData( vars=[0]):
        var_list = ','.join( [ ( '"v%d:%s"' % ( ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '{"%s":[%s]}' % ( MERRA_TEST_VARIABLES["collection"], var_list )
        return data

    def getLocalData():
        data = '{"MERRA/mon/atmos/hur":["v0:hur"]}'
        return data

    def getTaskArgs( op, vars=[0] ):
        task_args = { 'region': getRegion(), 'data': getData(vars), 'operation': json.dumps(op) }
        return task_args


    def getTaskArgsLoc( op ):
        task_args = { 'region': getRegion(), 'data': getLocalData(), 'operation': json.dumps(op) }
        return task_args


    def test_1():
        result = kernelMgr.run( TaskRequest( request={'config': {'cache': True}, 'region': {'level': 85000}, 'data': '{"name": "hur", "collection": "MERRA/mon/atmos", "id": "v0"}'} ) )
        result_stats = result[0]['result']
        pp.pprint(result_stats)

    def average():
        t0 = time.time()
        task_args = getTaskArgs( op=[operations[ 0 ]] )
        response = kernelMgr.run( TaskRequest( request=task_args ) )
        result_data = response['results'][0]['data']
        t1 = time.time()
        print "  Computed departures 1 in %.3f, results:" % (t1-t0)


    def test_average():
        t1 = time.time()
        task_args1 = getTaskArgs( op=[ ave_operations[0] ] )
        response1 = kernelMgr.run( TaskRequest( request=task_args1 ) )
        result_data = response1['results'][0]['data']
        t2 = time.time()
        print "  Computed departures (local data) in %.3f, results:" % (t2-t1)
        pp.pprint(result_data)


    def test_annual_cycle():
        task_args = getTaskArgs( op=[operations[ 1 ]] )
        response = kernelMgr.run( TaskRequest( request=task_args ) )
        result_data = response['results'][0]['data']
        pp.pprint(result_data)

    def test_value_retreval():
        task_args = getTaskArgs( op=[operations[ 2 ]] )
        response = kernelMgr.run( TaskRequest( request=task_args ) )
        result_data = response['results'][0]['data']
        pp.pprint(result_data)

    def test_multitask():
        task_args = getTaskArgs( op=operations )
        response = kernelMgr.run( TaskRequest( request=task_args ) )
        for ir, result in enumerate(response['results']):
            result_data = result['data']
            pp.pprint(result_data)

    def test_cache():
        task_args = { 'region': getCacheRegion(), 'data': getData() }
        response = kernelMgr.run( TaskRequest( request=task_args ) )
        pp.pprint(response)
        kernelMgr.persist()
#        response = kernelMgr.run( TaskRequest( request=task_args ) )
#        pp.pprint(response)

    def test_utilities( utility_id ):
        task_args = { 'region': getRegion(), 'data': getData() }
        response = kernelMgr.run( TaskRequest( request=task_args, utility=utility_id ) )
        pp.pprint(response)

    def test_uncache(  ):
        util_result = kernelMgr.run( TaskRequest( request={ 'region': getRegion(), 'data': getData() }, utility='domain.uncache' ) )
        response    = kernelMgr.run( TaskRequest( request={ 'region': {'level': 85000}, 'data': getData() } ) )
        pp.pprint(response)

    # def test_api_cache():
    #     request_parameters = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
    #     request_parameters['datainputs'] = [u'region={"id":"r0","level":{"value":"100000"}};data={"name":"hur","collection":"MERRA/mon/atmos","id":"v0","domain":"r0"}']
    #     response = kernelMgr.run( TaskRequest( request=request_parameters ) )
    #     pp.pprint(response)
    #
    # def test_api():
    #     request_parameters = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
    #     request_parameters['datainputs'] = [u'region={"id":"r0","level":{"value":"100000"}};data={"name":"hur","collection":"MERRA/mon/atmos","id":"v0","domain":"r0"};operation=["CDTime.departures(v0,slice:t)","CDTime.climatology(v0,slice:t,bounds:annualcycle)","CDTime.value(v0)"]']
    #     response = kernelMgr.run( TaskRequest( request=request_parameters ) )
    #     result_data = response['results'][1]['data']
    #     pp.pprint(result_data)
    #
    # def test_average():
    #     request_parameters = {'version': ['1.0.0'], 'service': ['WPS'], 'embedded': ['false'], 'rawDataOutput': ['result'], 'identifier': ['cdas'], 'request': ['Execute'] }
    #     request_parameters['datainputs'] = ['domain={"id":"r0","level":{"value":"100000"}};variable={"collection":"MERRA/mon/atmos","id":"v0:hur","domain":"r0"};operation=["CWT.average(v0,axis:xy)"]']
    #     response = kernelMgr.run( TaskRequest( request=request_parameters ) )
    #     pp.pprint(response)
    #
    # def test_new_api():
    #     requestData = {'config': {'cache': True}, 'domain': [ {'id':'r1','latitude': { 'value': 35.0 }, 'time': { 'value': u'2010-01-16T12:00:00' }, 'longitude': { 'value': -137.0 }, 'level': { 'value': 85000.0 } } ], 'data': [ {'dset':'MERRA/mon/atmos','id':'v0:hur','domain':'r1'} ], 'operation': [u'CDTime.departures(v0,slice:t)']}
    #     task_parms = dialect.getTaskRequestData( requestData )
    #     response = kernelMgr.run( TaskRequest( task=task_parms ) )
    #     result_data = response['results'][0]['data']
    #     pp.pprint(result_data)
