from modules.module import Executable
from modules.utilities import *
from datacache.manager import CachedVariable
from datacache.domains import Domain, Region
from request.manager import TaskRequest
import traceback

executionRecord = ExecutionRecord()

class ComputeEngine( Executable ):

    def __init__( self, id, **args ):
        Executable.__init__( self, id )
        self.communicator = self.getCommunicator()
        self.pendingTasks = {}
        self.cachedVariables = {}
        self.restore()

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
            executionRecord.clear()
            cache_task_request = None
            cache_task_monitor = None
            self.communicator.updateWorkerStats()
            self.processPendingTasks()
            designated_worker = None
            cached_domain = None
            datasets = task_request.data.values
            op_region = task_request.region.value
            utility = task_request['utility']
            operation = task_request.operations.values
            async = compute_args.get( 'async', ( not bool(operation) ) )
            wpsLog.debug( " ***** Executing compute engine (t=%.2f), async: %s, request: %s" % ( t0, async, str(task_request) ) )

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
                            cache_axis_list = [Region.LEVEL] if operation else [Region.LEVEL, Region.LATITUDE, Region.LONGITUDE]
                            cache_region = Region( op_region, axes=cache_axis_list )
                            cache_op_args = { 'region':cache_region.spec, 'data':str(dataset) }
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

                task_monitor = self.communicator.submitTask( task_request, designated_worker )
                op_domain = cached_var.addDomain( op_region )
                self.pendingTasks[ task_monitor ] = op_domain
                task_monitor.addStats( exerec=executionRecord.toJson() )

                wpsLog.debug( " ***** Sending operation [rid:%s] to worker '%s' (t = %.2f, dt0 = %.3f): request= %s " %  ( task_monitor.rid, str(designated_worker), t2, t2-t0, str(task_request) ) )
                if async: return task_monitor

                results = self.processOpTask( task_monitor )
                return results

            else:
                if async: return cache_task_request
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

    from request.manager import TaskRequest
    from engines import engineRegistry
    from modules.configuration import MERRA_TEST_VARIABLES, CDAS_COMPUTE_ENGINE
    test_point = [ -137.0, 35.0, 85000.0 ]
    test_time = '2010-01-16T12:00:00'
    operations = [ "CDTime.departures(v0,slice:t)", "CDTime.climatology(v0,slice:t,bounds:annualcycle)", "CDTime.value(v0)" ]
    import pprint
    pp = pprint.PrettyPrinter(indent=4)

    def getRegion():
        return '{"longitude": %.2f, "latitude": %.2f, "level": %.2f, "time":"%s" }' % (test_point[0],test_point[1],test_point[2],test_time)

    def getCacheRegion():
        return '{ "level": %.2f }' % (test_point[2])

    def getData( vars=[0]):
        var_list = ','.join( [ ( '"v%d:%s"' % ( ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '{"%s":[%s]}' % ( MERRA_TEST_VARIABLES["collection"], var_list )
        return data

    def getTaskArgs( op ):
        task_args = { 'region': getRegion(), 'data': getData(), 'operation': json.dumps(op) }
        return task_args

    def getCacheTaskArgs( ):
        task_args = { 'region': getCacheRegion(), 'data': getData() }
        return task_args

    run_cache = True
    run_op    = False
    engine = engineRegistry.getInstance( CDAS_COMPUTE_ENGINE + "Engine" )

    if run_cache:
        print " Running cache operation "
        ct0 = time.time()
        ctask_args = getCacheTaskArgs()
        ctask      = engine.execute( TaskRequest( request=ctask_args ), async=False )
        ct1 = time.time()
        print " Completed cache in %.2f sec " %  (ct1-ct0)

    if run_op:
        print " Running departures operation "
        t0 = time.time()
        task_args = getTaskArgs( op=operations[ 0:1 ] )
        results = engine.execute( TaskRequest( request=task_args ) )
        result_data = results[0]['data']
        t1 = time.time()
        print " Completed op in %.2f sec, data = %s " % ( (t1-t0), str( result_data ) )

    control_input = raw_input("Type <return> to continue. ")

    print "Sending shutdown"
    engine.execute( TaskRequest( utility='shutdown.all' ) )
    print "Sending request completed"