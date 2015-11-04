from modules.module import Executable
from modules.utilities import *
from datacache.manager import CachedVariable
from datacache.domains import Domain, Region, CDAxis
from request.manager import TaskRequest
from modules import configuration
import traceback

executionRecord = ExecutionRecord()

class ComputeEngine( Executable ):

    def __init__( self, id, **args ):
        Executable.__init__( self, id )
        self.communicator = self.getCommunicator()
        self.pendingTasks = {}
        self.cachedVariables = {}
        self.restore()
        self.invocation_index = 0

    def getCommunicator(self):
        return None

    def getCacheSizeMap(self):
        cache_map = {}
        for cvar in self.cachedVariables.values(): cvar.getCacheSize(cache_map)
        return cache_map

    def restore(self):
        wstat = self.communicator.getWorkerStats()
        cache_map = self.getCacheSizeMap()
        for wid, csize  in cache_map.items():  wstat['csize']  = csize
        self.loadStats()

    def processCacheTask(self, cache_task_monitor, cached_var, cached_domain, **args ):
        response = cache_task_monitor.response( **args )
        if not response:
            wpsLog.debug( " ***** Empty cache_request for task '%s', status = '%s':\n %s " % ( cache_task_monitor.id, cache_task_monitor.status(), str(cache_task_monitor) ) )
        else:
            worker = response['wid']
            results = response.get('results',None)
            if results: cached_var.loadStats( results['domain_spec'].variable_spec )
            cached_domain.cacheRequestComplete( worker )
            self.communicator.clearWorkerState( worker )
            del self.pendingTasks[ cache_task_monitor ]
            wpsLog.debug( " ***** process Completed Task: worker = %s, cache_request = %s " % ( worker, str(cache_task_monitor) ) )
        results = response['results']
        if isinstance( results, list ):
            for result in results: result.update( args )
        else: results.update( args )
        return results

    def processOpTask( self, task_monitor, **args  ):
        response = task_monitor.response( **args )
        worker = response['wid']
        wpsLog.debug( " ***** Retrieved result [rid:%s] from worker '%s'" %  ( task_monitor.rid, worker ) )
        self.communicator.clearWorkerState( worker )
        del self.pendingTasks[ task_monitor ]
        results = response['results']
        if isinstance( results, list ):
            for result in results: result.update( args )
        else:
            results.update( args )
        return results

    def processPendingTasks(self):
        pTasks = str( self.pendingTasks.keys() )
        t0 = time.time()
        completed_requests = []
        do_cache = False
        for task_monitor,cached_domain in self.pendingTasks.items():
            if task_monitor.ready():
                if task_monitor.status() == 'FAILURE':
                    wpsLog.debug( "Task %s(%s) Failed:\n>> '%s' " % ( task_monitor.taskName(), task_monitor.rid, str(task_monitor) ) )
                else:
                    response = task_monitor.response()
         #           wpsLog.debug( " PPT: got response = %s, cache_request = %s " % ( str(response), str(task_monitor) ) )
                    if not response:
                        wpsLog.debug( " PPT: Empty cache_request for task '%s', status = '%s':\n %s " % ( task_monitor.id, task_monitor.status(), str(task_monitor) ) )
                    else:
                        worker = response['wid']
                        results = response['results']
                        if results:
#                            wpsLog.debug( "\n\n PPT: process results '%s' \n\n" % str( results ) )
                            cvar = self.cachedVariables.get( results['cid'], None )
                            if cvar: cvar.loadStats(results)
                        cached_domain.cacheRequestComplete( worker )
                        completed_requests.append( task_monitor )
                        self.communicator.clearWorkerState( worker )
         #               wpsLog.debug( " PPT: process Completed Task: worker = %s, cache_request = %s " % ( worker, str(task_monitor) ) )
            else:
                wpsLog.debug( " PPT: Still waiting on Task: cache_request = %s " % ( str(task_monitor) ) )
        for completed_request in completed_requests:
            del self.pendingTasks[ completed_request ]
        t1 = time.time()
        wpsLog.debug( " ***** processPendingTasks[ dt = %0.3f ]: %s" %  ( t1-t0, pTasks ) )

    def loadStats( self ):
        worker_stats = self.getWorkerCacheStats()
        for worker_stat in worker_stats:
            wid = worker_stat['wid']
            cache_stats = worker_stat['stats']
            for var_stats in cache_stats:
                cache_id = var_stats.get('cid',None)
                cached_cvar = self.cachedVariables.get( cache_id, None )
                if cached_cvar is None:
                    cached_cvar = CachedVariable(  id=cache_id, variable_spec=var_stats )
                    self.cachedVariables[ cache_id ] = cached_cvar
                domain_stats = var_stats['domains']
                for domain_stat in domain_stats:
                    cached_cvar.addCachedDomain( domain_stat['region'], region_spec=domain_stat, variable_spec=var_stats )

    def findCachedDomain(self, var_cache_id, region, dataset=None ):
        cached_var = self.cachedVariables.get( var_cache_id, None )
        if cached_var == None:
            cached_var =  CachedVariable( id=var_cache_id, dataset=dataset)
            self.cachedVariables[ var_cache_id ] = cached_var
        overlap,domain = cached_var.findDomain( region )
        if overlap == Domain.CONTAINED: return cached_var, domain
        return cached_var, None

    def uncache( self, var_cache_id, region ):
        cached_var = self.cachedVariables.get( var_cache_id, None )
        return cached_var.uncache( region ) if cached_var is not None else []

    def  getWorkerCacheStats(self):
        cache_task_request = TaskRequest( utility='worker.cache' )
        cache_task_monitor = self.communicator.submitTask( cache_task_request, "*" )
        workerCacheStats = cache_task_monitor.response()
        return workerCacheStats

    def transferData(self, source_worker, destination_worker, domain ):
        cid = domain.stat['cid']
        cvar = self.cachedVariables[ cid ]
        transfer_task_request = TaskRequest( utility='domain.transfer', source=source_worker, destination=destination_worker, domain=str(domain), var=cid, spec=cvar.spec )
        self.communicator.submitTask( transfer_task_request, [ source_worker, destination_worker ] )

    def execute( self, task_request, **compute_args ):
        try:
            t0 = time.time()
            self.invocation_index += 1
            operation = task_request.operations.values
            embedded = task_request.getBoolRequestArg( 'embedded', False )
            async = task_request.getBoolRequestArg( 'async', True if operation else not embedded )
            wpsLog.debug( " ***** Executing compute engine (t=%.2f), async: %s, embedded: %s, request: %s" % ( t0, async, embedded, str(task_request) ) )

            executionRecord.clear()
            cache_task_request = None
            cache_task_monitor = None
            self.communicator.updateWorkerStats()
            self.processPendingTasks()
            designated_worker = None
            cached_domain = None
            cache_worker = None
            var_cache_id = None
            wait_for_cache = False
            datasets = task_request.data.values
            op_region = task_request.region.value
            utility = task_request['utility']

            if utility == "shutdown.all":
                shutdown_task_monitor = self.communicator.submitTask( task_request, "*" )
                self.communicator.close()
                return
            elif utility == "domain.transfer":
                task_monitor = self.communicator.submitTask( task_request, '*' )
                if async: return { }
                else: return task_monitor.response()
            else:
                for dataset in datasets:
                    dsid = dataset.get('name','')
                    collection = dataset.get('collection',None)
                    url = dataset.get('url','')
                    var_cache_id = ":".join( [collection,dsid] ) if (collection is not None) else ":".join( [url,dsid] )
                    if var_cache_id <> ":":
                        if utility == "domain.uncache":
                            wids = self.uncache( var_cache_id, op_region )
                            self.communicator.submitTask( task_request, wids )
                        else:
                            cached_var,cached_domain = self.findCachedDomain( var_cache_id, op_region, dataset )
                            wpsLog.debug( " Find Cached Domain, cached_var: %s, cached_domain: %s" % ( str(cached_var), str(cached_domain) ) )
                            if cached_domain is None:
                                cache_axis_list = [CDAxis.LEVEL] if operation else [CDAxis.LEVEL, CDAxis.LATITUDE, CDAxis.LONGITUDE, CDAxis.TIME]
                                cache_region = Region( op_region.filter_spec( cache_axis_list ) )
                                cache_op_args = { 'region':cache_region, 'data':str(dataset) }
                                tc0 = time.time()
                                cache_task_request = TaskRequest( task=cache_op_args )
                                cache_worker = self.communicator.getNextWorker(True)
                                cache_task_monitor = self.communicator.submitTask( cache_task_request, cache_worker )
                                executionRecord.addRecs( cache_add=cache_region.spec, cache_add_worker = cache_worker )
                                tc01 = time.time()
                                cached_domain = cached_var.addDomain( cache_region, queue=cache_worker )
                                self.pendingTasks[ cache_task_monitor ] = cached_domain
                                tc1 = time.time()
                                wpsLog.debug( " ***** Caching data [rid:%s] to worker '%s' ([%.2f,%.2f,%.2f] dt = %.3f): args = %s " %  ( cache_task_monitor.rid, cache_worker, tc0, tc01, tc1, (tc1-tc0), str(cache_op_args) ) )
                                if op_region == cache_region:
                                    designated_worker = cache_worker
                                    wait_for_cache = True
                            else:
                                worker_id, cache_request_status = cached_domain.getCacheStatus()
                                executionRecord.addRecs( cache_found=cache_request_status, cache_found_domain=cached_domain.spec, cache_found_worker=worker_id )
                                if (cache_request_status == Domain.COMPLETE) or ( cached_var.cacheType() == CachedVariable.CACHE_OP ):
                                    designated_worker = worker_id
                                    wpsLog.debug( " ***** Found cached data on worker %s " %  worker_id )
                                else:
                                    wpsLog.debug( " ***** Found cache op on worker %s, data not ready " %  worker_id )

            if operation:
                t2 = time.time()
                if designated_worker is None:
                    designated_worker = self.communicator.getNextWorker()
                else:
                    executionRecord.addRecs( designated= True, designated_worker= designated_worker )

                if async:
                    op_index = int(100*time.time())
                    result_names = [ "r%d-%d-%d.nc"%(ivar,self.invocation_index,op_index) for ivar in range(len(operation)) ]
                    task_request['result_names'] = result_names

                if wait_for_cache:
                    cache_results = self.processCacheTask( cache_task_monitor, cached_var, cached_domain, exerec=executionRecord.toJson() )

                task_monitor = self.communicator.submitTask( task_request, designated_worker )
                op_domain = cached_var.addDomain( op_region )
                self.pendingTasks[ task_monitor ] = op_domain
                task_monitor.addStats( exerec=executionRecord.toJson() )

                wpsLog.debug( " ***** Sending operation [rid:%s] to worker '%s' (t = %.2f, dt0 = %.3f): request= %s " %  ( task_monitor.rid, str(designated_worker), t2, t2-t0, str(task_request) ) )
                if async:
                    task_monitor.addStats( result_names=result_names, result_url=configuration.CDAS_OUTGOING_DATA_URL  )
                    return task_monitor

                results = self.processOpTask( task_monitor )
                wpsLog.debug( " ***** Received operation [rid:%s] from worker '%s': response= %s " %  ( task_monitor.rid, str(designated_worker), str(task_request) ) )

                return results

            else:
                cache_rval = { 'cache_worker': cache_worker, 'cache_region': cached_domain, 'cached_var': var_cache_id, 'async': async, 'exerec': executionRecord.toJson()  }
                if async: return cache_rval
                else:
                    if cache_task_monitor is not None: self.processCacheTask( cache_task_monitor, cached_var, cached_domain )
                    return cache_rval

        except Exception, err:
            wpsLog.error(" Error running compute engine: %s\n %s " % ( str(err), traceback.format_exc()  ) )
            return err


class EngineTests:

    def __init__(self):
        from engines import engineRegistry
        self.test_point = [ -137.0, 35.0, 85000 ]
        self.test_time = '2010-01-16T12:00:00'
        self.operations = [ "CDTime.departures(v0,slice:t)", "CDTime.climatology(v0,slice:t,bounds:annualcycle)", "CDTime.value(v0)" ]
        self.indexed_operations = [ "CDTime.departures($0,slice:t)", "CDTime.climatology($0,slice:t,bounds:annualcycle)", "CDTime.value($0)" ]
        self.def_task_args =  { "domain": self.getRegion(), "variable": self.getData(), 'embedded': True, 'async': False }
        self.engine = engineRegistry.getInstance( configuration.CDAS_COMPUTE_ENGINE + "Engine" )
        self.cache_region =  Region( { "lev": {"config":{},"bounds":[ self.test_point[2] ], 'id':"r0" } } )

    def tearDown(self):
        pass

    def getRegion(self, ipt=0 ):
        return '[{"id":"r0","longitude": {"value":%.2f,"system":"values"}, "latitude": %.2f, "level": %.2f, "time":"%s" }]' % (self.test_point[0]+5*ipt,self.test_point[1]-5*ipt,self.test_point[2],self.test_time)

    def getData(self, vars=[0]):
        var_list = ','.join( [ ( '{"dset":"%s","id":"v%d:%s","domain":"r0"}' % ( configuration.MERRA_TEST_VARIABLES["collection"], ivar, configuration.MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '[%s]' % ( var_list )
        return data

    def getLocalData(self):
        data = '{"dset":"MERRA/mon/atmos/hur","id":"v0:hur","domain":"r0"}'
        return data

    def getOp(self, op_index ):
        return [ self.operations[ op_index ] ]

    def getTaskArgs(self, op ):
        task_args = dict( self.def_task_args )
        task_args['operation'] = op
        return task_args

    def getResultData( self, results, index=0 ):
        if isinstance( results, Exception ):
            raise results
        if isinstance( results, list ): rdata = results[index].get( 'data', None )
        else:                           rdata = results.get( 'data', None )
        return None if (rdata is None) else ( [ float(rd) for rd in rdata ] if hasattr( rdata, '__iter__' ) else float(rdata) )

    def getResultStats( self, results, index=0 ):
        if isinstance( results, Exception ):
            print(str(results))
        return ExecutionRecord( results[index]['exerec'] )

    def transfer(self):
        result = self.engine.execute( TaskRequest( request={ 'domain': self.cache_region, 'variable': self.getData(), 'async': False } ) )
        wpsLog.debug( "\n\n ++++++++++++++++ ++++++++++++++++ ++++++++++++++++ Cache Result: %s\n\n ", str(result ) )
        cached_var, domain = self.engine.findCachedDomain( result['cached_var'], self.cache_region )
        task_args = { 'source': result['cache_worker'], 'destination': 'W-0', 'domain_spec': domain.getDomainSpec(), 'async': False, 'embedded': True  }
        t0 = time.time()
        results = self.engine.execute( TaskRequest( utility='domain.transfer', request=task_args ) )
        t1 = time.time()
        wpsLog.debug( "\n\n ++++++++++++++++ ++++++++++++++++ ++++++++++++++++ Transfer (dt = %0.2f) Results: %s\n\n " % (t1-t0, str(results) ) )

    def xtest02_departures(self):
        test_result = [  -1.405364990234375, -1.258880615234375, 0.840728759765625, 2.891510009765625, -18.592864990234375,
                        -11.854583740234375, -3.212005615234375, -5.311614990234375, 5.332916259765625, -1.698333740234375,
                          8.750885009765625, 11.778228759765625, 12.852447509765625 ]
        task_args = self.getTaskArgs( op=[ self.indexed_operations[0] ] )
        result = self.engine.execute( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result )
        compute_result = result_data[0:len(test_result)]

    def xtest03_annual_cycle(self):
        test_result = [48.07984754774306, 49.218166775173614, 49.36114501953125, 46.40715196397569, 46.3406982421875, 44.37486775716146, 46.54383680555556, 48.780619303385414, 46.378028021918404, 46.693325466579864, 48.840003119574654, 46.627953423394096]
        task_args = self.getTaskArgs( op=self.getOp( 1 ) )
        result = self.engine.execute( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result )
        self.assertEqual( test_result, result_data[0:len(test_result)] )

    def xtest04_value_retreval(self):
        test_result = 59.765625
        task_args = self.getTaskArgs( op=self.getOp( 2 ) )
        result = self.engine.execute( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result )

    def xtest05_multitask(self):
        test_results = [ [ -1.405364990234375, -1.258880615234375, 0.840728759765625 ], [48.07984754774306, 49.218166775173614, 49.36114501953125], 59.765625 ]
        task_args = self.getTaskArgs( op=self.operations )
        results = self.engine.execute( TaskRequest( request=task_args ) )
        for ir in range(len(results)):
            rdata = self.getResultData( results, ir )
            test_result = test_results[ir]

if __name__ == '__main__':
    test_runner = EngineTests()
    test_runner.transfer()
    print "Done!"
