from modules import Executable
from tasks import execute, simpleTest
from modules.utilities import *
from datacache.manager import CachedVariable
from datacache.status_cache import  StatusCacheManager
from datacache.domains import Domain
import celery, pickle, os, traceback
cLog = logging.getLogger('celery-debug')
cLog.setLevel(logging.DEBUG)
if len( cLog.handlers ) == 0: cLog.addHandler( logging.FileHandler( os.path.expanduser( "~/.celery-debug") ) )

StatusCache = StatusCacheManager()

class CeleryEngine( Executable ):

    def __init__( self, id ):
        Executable.__init__( self, id )
        self.worker_specs = {}
        self.cachedVariables = {}
        self.workerQueueIndex = 0
        self.pendingTasks = {}
        self.statusCache = StatusCacheManager()
        self.restore()

    def restore(self):
        cache_data = self.statusCache.restore( 'cdas_celery_engine_cache' )
        if cache_data:
            [ self.worker_specs, self.cachedVariables, self.workerQueueIndex, self.pendingTasks ] = cache_data

    def cache(self):
        self.statusCache.cache( 'cdas_celery_engine_cache', [ self.worker_specs, self.cachedVariables, self.workerQueueIndex, self.pendingTasks ] )

    def processPendingTasks(self):
        pTasks = str( self.pendingTasks.keys() )
        t0 = time.time()
        completed_requests = []
        do_cache = False
        for cache_request,cached_domain in self.pendingTasks.items():
            if cache_request.ready():
                result = cache_request.get()
                worker = result['worker']
                cached_domain.cacheRequestComplete( worker )
                completed_requests.append( cache_request )
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
        workers = self.worker_specs.keys()
        worker_addr = workers[ self.workerQueueIndex % len(workers) ]
        self.WorkerQueueIndex = self.workerQueueIndex + 1
        return worker_addr    # .split('@')[1]

    def execute( self, run_args ):
        try:
            t0 = time.time()
            self.updateWorkerSpecs()
            self.processPendingTasks()
            designated_worker = None
            cLog.debug( " ***** Executing Celery engine (t=%.2f), args: %s" % ( t0, run_args ) )
            var_mdata = get_json_arg( 'data', run_args )
            region = get_json_arg( 'region', run_args )
            operation = get_json_arg( 'operation', run_args )
            id = var_mdata.get('id','')
            collection = var_mdata.get('collection',None)
            url = var_mdata.get('url','')
            var_cache_id = ":".join( [collection,id] ) if (collection is not None) else ":".join( [url,id] )
            if var_cache_id <> ":":
                cached_var,cached_domain = self.findCachedDomain( var_cache_id, region, run_args )
                if cached_domain is None:
                    if operation:
                        cache_op_args = { 'data':var_mdata }
                        cache_region = None
                    else:
                        cache_op_args = { 'region':region, 'data':var_mdata }
                        cache_region = region
                    tc0 = time.time()
                    cache_worker = self.getNextWorker()
                    cache_request = execute.apply_async( (cache_op_args,), exchange='C.dq', routing_key=cache_worker )
                    tc01 = time.time()
                    cached_domain = cached_var.addDomain( cache_region )
                    self.pendingTasks[ cache_request ] = cached_domain
                    self.cache()
                    tc1 = time.time()
                    cLog.debug( " ***** Caching data [tid:%s] to worker '%s' ([%.2f,%.2f,%.2f] dt = %.3f): args = %s " %  ( cache_request.id, cache_worker, tc0, tc01, tc1, (tc1-tc0), str(cache_op_args) ) )
                else:
                    worker_id, cache_request_status = cached_domain.getCacheStatus()
                    if cache_request_status == Domain.COMPLETE:
                        designated_worker = worker_id
                        cLog.debug( " ***** Found cached data on worker %s " %  worker_id )
                    else:
                        cLog.debug( " ***** Found cache op on worker %s, data not ready " %  worker_id )

            if operation:
                t2 = time.time()

                if designated_worker is None:
                    task = execute.delay( run_args )
                else:
                    task = execute.apply_async( (run_args,), exchange='C.dq', routing_key=designated_worker )

                op_domain = cached_var.addDomain( region )
                self.pendingTasks[ task ] = op_domain
                self.cache()

                cLog.debug( " ***** Sending operation [tid:%s] to worker '%s' (t = %.2f, dt0 = %.3f): args= %s " %  ( task.id, str(designated_worker), t2, t2-t0, str(run_args) ) )

                # time.sleep(1.0)
                # celery_inspect = celery.current_app.control.inspect()
                # print( "Celery execution stats:\n " )
                # pp.pprint( celery_inspect.stats() )
                # print( "Active tasks:\n " )
                # pp.pprint( celery_inspect.active() )
                # print( "Reserved tasks:\n " )
                # pp.pprint( celery_inspect.reserved() )

                result = task.get()
                t1 = time.time()
                cLog.debug( " ***** Retrieved result [tid:%s] from worker '%s' (t = %.2f, dt1 = %.3f)" %  ( task.id, result['worker'], t1, t1-t2 ) )
                return result
        except Exception, err:
            wpsLog.error(" Error running celery engine: %s\n %s " % ( str(err), traceback.format_exc()  ) )
            return ""



if __name__ == "__main__":
    import sys, pprint
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)
    test_cache = False
    simple_test = False

    variable =  { 'collection': 'MERRA/mon/atmos', 'id': 'clt' }

    region1    = { "longitude":-24.20, "latitude":58.45 }
    region2    = { "longitude":-30.20, "latitude":67.45 }
    cache_region    = { "longitude": [ -60.0, 0.0 ], "latitude": [ 30.0, 90.0 ] }

    op_annual_cycle =  {"kernel":"time", "type":"climatology", "bounds":"annualcycle"}
    op_departures =  {"kernel":"time", "type":"departures",  "bounds":"np"}

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
            t0 = time.time()
            run_args1 = { 'data': variable, 'region':region1, 'operation': op_departures }
            result1 = engine.execute( run_args1 )

            print "\n ---------- Result (departures): ---------- "
            pp.pprint(result1)
            t1 = time.time()
            run_args2 = { 'data': variable, 'region':region1, 'operation': op_annual_cycle }
            result2 = engine.execute( run_args2 )

            print "\n ---------- Result(annual cycle): ---------- "
            pp.pprint(result2)
            t2 = time.time()
            print "\n Operations Complete, dt0 = %.2f, dt1 = %.2f, dt = %.2f  " % ( t1-t0, t2-t1, t2-t0 )
