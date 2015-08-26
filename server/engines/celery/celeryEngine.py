from modules import Executable
from tasks import execute, simpleTest
from modules.utilities import *
from datacache.manager import CachedVariable
from datacache.status_cache import  StatusCacheManager
from request.manager import TaskRequest
from datacache.domains import Domain, Region
from request.manager import TaskRequest
import celery, pickle, os, traceback
import sys, pprint
cLog = logging.getLogger('celery-debug')
cLog.setLevel(logging.DEBUG)
if len( cLog.handlers ) == 0: cLog.addHandler( logging.FileHandler( os.path.expanduser( "~/.celery-debug") ) )

StatusCache = StatusCacheManager()

class CeleryEngine( Executable ):

    WSTAT_FREE = 1
    WSTAT_OP = 2
    WSTAT_CACHE = 3

    def __init__( self, id ):
        Executable.__init__( self, id )
        self.worker_specs = {}
        self.cachedVariables = {}
        self.pendingTasks = {}
        self.statusCache = StatusCacheManager()
        self.restore()

    def setWorkerStatus( self, wid, status ):
        wspec = self.worker_specs.get( wid, None )
        if ( wspec is None ):
            cLog.error( "Unrecognized worker ID in 'setWorkerStatus[%d]': %s" % ( status, wid ) )
        else:
            wspec['status'] = status
            wspec['tstamp'] = time.time()
        if status == self.WSTAT_CACHE:
            wspec['csize'] = wspec.get('csize', 0 ) + 1


    def getWorkerStatus( self, wid ):
        wspec = self.worker_specs.get( wid, None )
        if ( wspec is None ):
            cLog.error( "Unrecognized worker ID in 'getWorkerStatus': %s" % ( wid ) )
            return None
        else:
            return wspec.get('status', self.WSTAT_FREE )

    # def updateWorkerCacheSize( self, wid, additional_cache_size ):
    #     wspec = self.worker_specs.get( wid, None )
    #     if ( wspec is None ):
    #         cLog.error( "Unrecognized worker ID in 'updateWorkerCacheSize': %s" % ( wid ) )
    #     else:
    #         wspec['csize'] = wspec.get('csize', 0 ) + additional_cache_size
    #
    # def getWorkerCacheSize( self, wid ):
    #     wspec = self.worker_specs.get( wid, None )
    #     if ( wspec is None ):
    #         cLog.error( "Unrecognized worker ID in 'getWorkerCacheSize': %s" % ( wid ) )
    #         return None
    #     else:
    #         return wspec.get('csize', 0 )

    def restore(self):
        cache_data = self.statusCache.restore( 'cdas_celery_engine_cache' )
        if cache_data:
            [ self.worker_specs, self.cachedVariables,  self.pendingTasks ] = cache_data

    def cache(self):
        self.statusCache.cache( 'cdas_celery_engine_cache', [ self.worker_specs, self.cachedVariables, self.pendingTasks ] )

    def processPendingTasks(self):
        pTasks = str( self.pendingTasks.keys() )
        t0 = time.time()
        completed_requests = []
        do_cache = False
        for cache_request,cached_domain in self.pendingTasks.items():
            if cache_request.ready():
                if cache_request.status == 'FAILURE':
                    cLog.debug( "Task %s(%s) Failed:\n>> '%s'  \n>> %s " % ( cache_request.task_name, cache_request.id, cache_request.result,  cache_request.traceback ) )
                else:
                    results = cache_request.get()
                    if not results:
                        cLog.debug( " ***** Empty cache_request for task '%s', status = '%s':\n %s " % ( cache_request.id, cache_request.status, str(cache_request) ) )
                    else:
                        worker = results[0]['worker']
                        cached_domain.cacheRequestComplete( worker )
                        completed_requests.append( cache_request )
                        self.setWorkerStatus( worker, self.WSTAT_FREE )
                        cLog.debug( " ***** process Completed Task: worker = %s, cache_request = %s " % ( worker, str(cache_request) ) )
        for completed_request in completed_requests:
            del self.pendingTasks[ completed_request ]
            do_cache = True
        if do_cache: self.cache()
        t1 = time.time()
        cLog.debug( " ***** processPendingTasks[ dt = %0.3f ]: %s" %  ( t1-t0, pTasks ) )

    def updateWorkerSpecs(self):
        t0 = time.time()
        if len( self.worker_specs ) == 0:
            self.worker_specs = celery.current_app.control.inspect().stats()
        if self.worker_specs == None:
            wpsLog.error( "ERROR: Must start up celery workers!" )
            self.worker_specs = {}
        t1 = time.time()
        cLog.debug( " ***** updateWorkerSpecs[ dt = %0.3f ], workers: %s" %  ( t1-t0, str(self.worker_specs.keys()) ) )

    def findCachedDomain(self, var_cache_id, region, var_specs ):
        cached_var = self.cachedVariables.setdefault( var_cache_id, CachedVariable( id=var_cache_id, specs=var_specs) )
        if ( cached_var is not None ):
            overlap,domain = cached_var.findDomain( region )
            if overlap == Domain.CONTAINED: return cached_var, domain
        return cached_var, None

    def getNextWorker( self ):
        mdbg = False
        if mdbg: cLog.debug( "  %%%%%%%%%%%%%%%% GetNextWorker: ")
        operational_worker = None
        free_worker = None
        op_worker_tstamp = float('Inf')
        free_worker_cache_size = float('Inf')
        for worker, wspec in self.worker_specs.items():
            wstatus =  wspec.get('status', self.WSTAT_FREE )
            if mdbg: cLog.debug( "  >>-----> Worker: '%s', status: %d " % ( worker, wstatus) )
            if wstatus == self.WSTAT_FREE:
                cache_size =  wspec.get( 'csize', 0 )
                if(cache_size < free_worker_cache_size):
                   free_worker =  worker
                   free_worker_cache_size = cache_size
            elif wstatus == self.WSTAT_OP:
                ts = wspec['tstamp']
                if(ts < op_worker_tstamp):
                   operational_worker =  worker
                   op_worker_tstamp = ts
        return free_worker if free_worker is not None else operational_worker

    def execute( self, task_request, **celery_args ):
        try:
            t0 = time.time()
            async = celery_args.get( 'async', False )
            cache_request = None
            self.updateWorkerSpecs()
            self.processPendingTasks()
            designated_worker = None
            cLog.debug( " ***** Executing Celery engine (t=%.2f), request: %s" % ( t0, str(task_request) ) )
            dset_mdata = task_request.data.values
            op_region = task_request.region.value
            operation = task_request.operation.values

            for var_mdata in dset_mdata:
                id = var_mdata.get('id','')
                collection = var_mdata.get('collection',None)
                url = var_mdata.get('url','')
                var_cache_id = ":".join( [collection,id] ) if (collection is not None) else ":".join( [url,id] )
                if var_cache_id <> ":":
                    cached_var,cached_domain = self.findCachedDomain( var_cache_id, op_region, task_request )
                    if cached_domain is None:
                        if operation:
                            region = Region( { 'level': op_region.getAxisRange('lev') } )
                            cache_op_args = { 'region':region.spec, 'data':var_mdata.spec }
                            cache_region = region
                        else:
                            cache_op_args = { 'region':op_region.spec, 'data':var_mdata.spec }
                            cache_region = op_region
                        tc0 = time.time()
                        cache_task_request = TaskRequest( task=cache_op_args)
                        cache_worker = self.getNextWorker()
                        cache_request = execute.apply_async( (cache_task_request.task,), exchange='C.dq', routing_key=cache_worker )
                        tc01 = time.time()
                        self.setWorkerStatus( cache_worker, self.WSTAT_CACHE )
                        cached_domain = cached_var.addDomain( cache_region )
                        self.pendingTasks[ cache_request ] = cached_domain
                        self.cache()
                        tc1 = time.time()
                        cLog.debug( " ***** Caching data [tid:%s] to worker '%s' ([%.2f,%.2f,%.2f] dt = %.3f): args = %s " %  ( cache_request.id, cache_worker, tc0, tc01, tc1, (tc1-tc0), str(cache_op_args) ) )
                    else:
                        worker_id, cache_request_status = cached_domain.getCacheStatus()
                        if (cache_request_status == Domain.COMPLETE) or ( cached_var.cacheType() == CachedVariable.CACHE_OP ):
                            designated_worker = worker_id
                            cLog.debug( " ***** Found cached data on worker %s " %  worker_id )
                        else:
                            cLog.debug( " ***** Found cache op on worker %s, data not ready " %  worker_id )

            if operation:
                t2 = time.time()

                if designated_worker is None:
                    designated_worker = self.getNextWorker()

                task = execute.apply_async( (task_request.task,), exchange='C.dq', routing_key=designated_worker )

                self.setWorkerStatus( designated_worker, self.WSTAT_OP )
                op_domain = cached_var.addDomain( op_region )
                self.pendingTasks[ task ] = op_domain
                self.cache()

                cLog.debug( " ***** Sending operation [tid:%s] to worker '%s' (t = %.2f, dt0 = %.3f): request= %s " %  ( task.id, str(designated_worker), t2, t2-t0, str(task_request) ) )

                if async: return task


                result = task.get()
                t1 = time.time()
                cLog.debug( " ***** Retrieved result [tid:%s] from worker '%s' (t = %.2f, dt1 = %.3f)" %  ( task.id, result[0]['worker'], t1, t1-t2 ) )
                return result

            else:
                if async: return cache_request

        except Exception, err:
            wpsLog.error(" Error running celery engine: %s\n %s " % ( str(err), traceback.format_exc()  ) )
            return None


    def inspect(self):
        pp = pprint.PrettyPrinter(indent=4)
        celery_inspect = celery.current_app.control.inspect()
        print( "Celery execution stats:\n " )
        pp.pprint( celery_inspect.stats() )
        print( "Active tasks:\n " )
        pp.pprint( celery_inspect.active() )
        print( "Reserved tasks:\n " )
        pp.pprint( celery_inspect.reserved() )


def run_test():
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)
    test_cache = False
    simple_test = False

    variable =   { 'collection': 'MERRA/mon/atmos', 'id': 'clt' }

    region1    = { "longitude":-24.20, "latitude":58.45, "level": 10000.0 }
    region2    = { "longitude":-30.20, "latitude":67.45, "level": 8500.0 }
    op_annual_cycle =   {"kernel":"time", "type":"climatology", "bounds":"annualcycle"}
    op_departures =   {"kernel":"time", "type":"departures",  "bounds":"np"}

    engine = CeleryEngine('celery')

    if simple_test:
        engine.updateWorkerSpecs()
        cache_worker = engine.getNextWorker()
        args = [ 1,2,3]
        ts0 = time.time()
        task = simpleTest.apply_async( (args,),  exchange='C.dq', routing_key=cache_worker )
        result = task.get()
        ts1 = time.time()
        print( " ***** Simple test using worker %s, time = %.3f " %  ( cache_worker, (ts1-ts0) ) )
    else:
        if test_cache:
            engine.updateWorkerSpecs()
            cache_worker = engine.getNextWorker()
            print( " ***** Caching data to worker %s " %  cache_worker )
            cache_op_args = { 'data': variable }
            task = execute.apply_async( (cache_op_args,True), exchange='C.dq', routing_key=cache_worker )
            result = task.get()
            print "\n ---------- Result(cache): ---------- "
            pp.pprint(result)
        else:
            run_async = True
            if run_async:
                t0 = time.time()
                request = TaskRequest( task={ 'data': variable, 'region':region1, 'operation': op_departures } )
                task1 = engine.execute( request, async=True )

                request = TaskRequest( task={ 'data': variable, 'region':region2, 'operation': op_annual_cycle } )
                task2 = engine.execute( request, async=True )

                t1 = time.time()

                result1 = task1.get() if task1 else None
                print "\n ---------- Result (departures): ---------- "
                pp.pprint(result1) if result1 else "<NONE>"
                t2 = time.time()

                print "\n ---------- Result (annual cycle): ---------- "
                result2 = task2.get() if task2 else None
                pp.pprint(result2) if result2 else "<NONE>"
                t3 = time.time()
                print "\n Operations Complete, dt0 = %.2f, dt1 = %.2f, , dt2 = %.2f, dt = %.2f  " % ( t1-t0, t2-t1, t3-t2, t3-t0 )
            else:
                t0 = time.time()
                request = TaskRequest( task={ 'data': variable, 'region':region1, 'operation': op_departures } )
                result1 = engine.execute( request )

                print "\n ---------- Result (departures): ---------- "
                pp.pprint(result1)
                t1 = time.time()

                request = TaskRequest( task={ 'data': variable, 'region':region1, 'operation': op_annual_cycle } )
                result2 = engine.execute( request )

                print "\n ---------- Result(annual cycle): ---------- "
                pp.pprint(result2)
                t2 = time.time()
                print "\n Operations Complete, dt0 = %.2f, dt1 = %.2f, dt = %.2f  " % ( t1-t0, t2-t1, t2-t0 )

if __name__ == "__main__":
#    import cProfile
#    cProfile.run( 'run_test()', None, 'time' )

    run_test()


