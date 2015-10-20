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

    def processCacheTask(self, cache_task_monitor, cached_domain, **args ):
        response = cache_task_monitor.response( **args )
        if not response:
            wpsLog.debug( " ***** Empty cache_request for task '%s', status = '%s':\n %s " % ( cache_task_monitor.id, cache_task_monitor.status(), str(cache_task_monitor) ) )
        else:
            worker = response['wid']
            cached_domain.cacheRequestComplete( worker )
            self.communicator.clearWorkerState( worker )
            del self.pendingTasks[ cache_task_monitor ]
            wpsLog.debug( " ***** process Completed Task: worker = %s, cache_request = %s " % ( worker, str(cache_task_monitor) ) )
        results = response['results']
        for result in results: result.update( args )
        return results

    def processOpTask( self, task_monitor, **args  ):
        response = task_monitor.response( **args )
        worker = response['wid']
        wpsLog.debug( " ***** Retrieved result [rid:%s] from worker '%s'" %  ( task_monitor.rid, worker ) )
        self.communicator.clearWorkerState( worker )
        del self.pendingTasks[ task_monitor ]
        results = response['results']
        for result in results: result.update( args )
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
                    cached_cvar = CachedVariable(  id=cache_id, vstat=var_stats )
                    self.cachedVariables[ cache_id ] = cached_cvar
                domain_stats = var_stats['domains']
                for domain_stat in domain_stats:
                    cached_cvar.addCachedDomain( domain_stat['region'], dstat=domain_stat, vstat=var_stats )

    def findCachedDomain(self, var_cache_id, region, dataset ):
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

    def execute( self, task_request, **compute_args ):
        try:
            t0 = time.time()
            self.invocation_index += 1
            operation = task_request.operations.values
            embedded = task_request.getBoolRequestArg( 'embedded', False )
            async = compute_args.get( 'async', not embedded )
            wpsLog.debug( " ***** Executing compute engine (t=%.2f), async: %s, embedded: %s, request: %s" % ( t0, async, embedded, str(task_request) ) )

            executionRecord.clear()
            cache_task_request = None
            cache_task_monitor = None
            self.communicator.updateWorkerStats()
            self.processPendingTasks()
            designated_worker = None
            cached_domain = None
            wait_for_cache = False
            datasets = task_request.data.values
            op_region = task_request.region.value
            utility = task_request['utility']

            if utility == "shutdown.all":
                shutdown_task_monitor = self.communicator.submitTask( task_request, "*" )
                self.communicator.close()
                return

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
                    cache_results = self.processCacheTask( cache_task_monitor, cached_domain, exerec=executionRecord.toJson() )

                task_monitor = self.communicator.submitTask( task_request, designated_worker )
                op_domain = cached_var.addDomain( op_region )
                self.pendingTasks[ task_monitor ] = op_domain
                task_monitor.addStats( exerec=executionRecord.toJson() )

                wpsLog.debug( " ***** Sending operation [rid:%s] to worker '%s' (t = %.2f, dt0 = %.3f): request= %s " %  ( task_monitor.rid, str(designated_worker), t2, t2-t0, str(task_request) ) )
                if async:
                    task_monitor.addStats( result_names=result_names, result_url=configuration.CDAS_OUTGOING_DATA_URL  )
                    return task_monitor

                results = self.processOpTask( task_monitor )
                return results

            else:
                if async: return cache_task_request.task
                else:
                    if cache_task_monitor is not None:
                        results = self.processCacheTask( cache_task_monitor, cached_domain, exerec=executionRecord.toJson() )
                        return results
                    else:
                        return [ { 'exerec': executionRecord.toJson() } ]

        except Exception, err:
            wpsLog.error(" Error running compute engine: %s\n %s " % ( str(err), traceback.format_exc()  ) )
            return err


if __name__ == '__main__':

    from engines import engineRegistry
    from modules.configuration import MERRA_TEST_VARIABLES, MERRA_LOCAL_TEST_VARIABLES, CDAS_COMPUTE_ENGINE
    test_point = [ -137.0, 35.0, 85000 ]
    test_time = '2010-01-16T12:00:00'
    operations = [ "CDTime.departures(v0,slice:t)", "CDTime.climatology(v0,slice:t,bounds:annualcycle)", "CDTime.value(v0)" ]
    engine = engineRegistry.getInstance( CDAS_COMPUTE_ENGINE + "Engine" )

    def getRegion( ipt=0 ):
        return '[{"id":"r0","longitude": {"value":%.2f,"system":"values"}, "latitude": %.2f, "level": %.2f, "time":"%s" }]' % (test_point[0]+5*ipt,test_point[1]-5*ipt,test_point[2],test_time)

    def getData( vars=[0]):
        var_list = ','.join( [ ( '{"dset":"%s","id":"v%d:%s","domain":"r0"}' % ( MERRA_TEST_VARIABLES["collection"], ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '[%s]' % ( var_list )
        return data

    def getLocalData():
        return '{"dset":"%s","id":"v0:%s","domain":"r0"}' % ( MERRA_LOCAL_TEST_VARIABLES["collection"], MERRA_LOCAL_TEST_VARIABLES["vars"][0] )

    def getOp( op_index ):
        return [ operations[ op_index ] ]

    def getTaskArgs( op ):
        task_args =  { "domain": getRegion(), "variable": getLocalData(), 'embedded': True }
        task_args['operation'] = op
        return task_args

    def getResultData(  results, index=0 ):
        if isinstance( results, Exception ):
            raise results
        rdata = results[index].get( 'data', None )
        return None if (rdata is None) else ( [ float(rd) for rd in rdata ] if hasattr( rdata, '__iter__' ) else float(rdata) )

    def getResultStats(  results, index=0 ):
        if isinstance( results, Exception ):
            print str(results)
        return ExecutionRecord( results[index]['exerec'] )

    def test01_cache():
        result      = engine.execute( TaskRequest( request={ 'domain': cache_region, 'variable': getLocalData() } ) )
        if isinstance( result, list ): result = result[0]
        print result

    def test02_departures():
        task_args = getTaskArgs( op=getOp( 0 ) )
        result = engine.execute( TaskRequest( request=task_args ) )
        result_data = getResultData( result )
        compute_result = result_data[0:10]
        print compute_result
        #        assertStatusEquals( result, cache_found=Domain.COMPLETE, cache_found_domain=cache_region,  designated=True )

    def xtest03_annual_cycle():
        task_args = getTaskArgs( op=getOp( 1 ) )
        result = engine.execute( TaskRequest( request=task_args ) )
        result_data = getResultData( result )
        print result_data[0:10]

    def xtest04_value_retreval():
        task_args = getTaskArgs( op=getOp( 2 ) )
        result = engine.execute( TaskRequest( request=task_args ) )
        result_data = getResultData( result )
        print result_data

    def xtest05_multitask():
        task_args = getTaskArgs( op=operations )
        results = engine.execute( TaskRequest( request=task_args ) )
        for ir in range(len(results)):
            rdata = getResultData( results, ir )
            print rdata[0:10]

    cache_region =  Region( { "lev": {"config":{},"bounds":[ test_point[2] ] } } )

    test01_cache()