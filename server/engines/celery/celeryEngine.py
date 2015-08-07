from modules import Executable
from tasks import execute
from modules.utilities import *
from datacache.manager import CachedVariable
from datacache.domains import Domain
import celery, pickle, os


class CeleryEngine( Executable ):

    def __init__( self, id ):
        Executable.__init__( self, id )
        self.worker_specs = None
        self.cachedVariables = {}
        self.workerQueueIndex = 0
        self.pendingTasks = {}
        self.cacheFile = os.path.expanduser( "~/.cdas_celery_engine_cache")
        self.restore()

    def restore(self):
        try:
            f = open( self.cacheFile )
            [ self.cachedVariables, self.workerQueueIndex, self.pendingTasks ] = pickle.load(f)
        except IOError, err:
            wpsLog.error( " Error reading cache file '%s': %s" % ( self.cacheFile, str(err) ) )
        except EOFError:
            wpsLog.warning( " Empty cache file '%s'" % ( self.cacheFile  ) )

    def cache(self):
        try:
            f = open( self.cacheFile, 'w' )
            pickle.dump( [ self.cachedVariables, self.workerQueueIndex, self.pendingTasks ], f )
        except IOError, err:
            wpsLog.error( " Error writing to cache file '%s': %s" % ( self.cacheFile, str(err) ) )

    def processPendingTasks(self):
        wpsLog.info( " ***** processPendingTasks: %s" % str( self.pendingTasks.keys() ) )
        completed_requests = []
        do_cache = False
        for cache_request,cached_domain in self.pendingTasks.items():
            if cache_request.ready():
                result = cache_request.get()
                worker = result['worker']
                cached_domain.cacheRequestComplete( worker )
                completed_requests.append( cache_request )
                wpsLog.info( " ***** process Completed Task: worker = %s, cache_request = %s " % ( worker, str(cache_request) ) )
        for completed_request in completed_requests:
            del self.pendingTasks[ completed_request ]
            do_cache = True
        if do_cache: self.cache()

    def updateWorkerSpecs(self):
        if self.worker_specs is None:
            self.worker_specs = celery.current_app.control.inspect().stats()
        if self.worker_specs == None:
            wpsLog.error( "ERROR: Must start up celery workers!" )

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
        debug = True
        self.updateWorkerSpecs()
        self.processPendingTasks()
        designated_worker = None
        wpsLog.info( " ***** Executing Celery engine, args: %s" % ( run_args ) )
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
                cache_worker = self.getNextWorker()
                if debug: print( " ***** Caching data to worker %s " %  cache_worker )
                cache_request = execute.apply_async( (cache_op_args,), exchange='C.dq', routing_key=cache_worker )
                cached_domain = cached_var.addDomain( cache_region )
                self.pendingTasks[ cache_request ] = cached_domain
                self.cache()
            else:
                worker_id, cache_request_status = cached_domain.getCacheStatus()
                if cache_request_status == Domain.COMPLETE:
                    designated_worker = worker_id
                    if debug: print( " ***** Found cached data on worker %s " %  worker_id )
                else:
                    if debug: print( " ***** Found cache op on worker %s, data not ready " %  worker_id )

        if operation:
            if designated_worker is None:
                task = execute.delay( run_args )
            else:
                task = execute.apply_async( (run_args,), exchange='C.dq', routing_key=designated_worker )

            if debug: print( " ***** Sending operation to worker %s, args: %s " %  ( str(designated_worker), str(run_args) ) )

            if debug:
                time.sleep(1.0)
                celery_inspect = celery.current_app.control.inspect()
                print( "Celery execution stats:\n " )
                pp.pprint( celery_inspect.stats() )
                print( "Active tasks:\n " )
                pp.pprint( celery_inspect.active() )
                print( "Reserved tasks:\n " )
                pp.pprint( celery_inspect.reserved() )

            result = task.get()
            if debug: print( " ***** Retrieved result from worker %s " %  result['worker'] )
            return result


if __name__ == "__main__":
    import sys, pprint
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)
    test_cache = False

    variable =  { 'collection': 'MERRA/mon/atmos', 'id': 'clt' }

    region1    = { "longitude":-24.20, "latitude":58.45 }
    region2    = { "longitude":-30.20, "latitude":67.45 }
    cache_region    = { "longitude": [ -60.0, 0.0 ], "latitude": [ 30.0, 90.0 ] }

    op_annual_cycle =  {"kernel":"time", "type":"climatology", "bounds":"annualcycle"}
    op_departures =  {"kernel":"time", "type":"departures",  "bounds":"np"}

    engine = CeleryEngine('celery')

    if test_cache:
        engine.updateWorkerSpecs()
        cache_worker = engine.getNextWorker()
        print( " ***** Caching data to worker %s " %  cache_worker )
        cache_op_args = { 'data': variable }
        task = execute.apply_async( (cache_op_args,), exchange='C.dq', routing_key=cache_worker )
        result = task.get()
    else:
        run_args = { 'data': variable, 'region':region1, 'operation': op_departures }
        result = engine.execute( run_args )

    print "\n ---------- Result: ---------- "
    pp.pprint(result)
