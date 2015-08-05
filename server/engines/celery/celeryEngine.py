from modules import Executable
from tasks import execute
from modules.utilities import *
from datacache.manager import CachedVariable
from datacache.domains import Domain
import celery


class CeleryEngine( Executable ):

    PendingTasks = {}
    CachedVariables = {}
    WorkerQueueIndex = 0
    NumWorkers = 4

    def processPendingTasks(self):
        completed_requests = []
        for cache_request,cached_domain in self.PendingTask.items():
            if cache_request.ready():
                cached_domain.cacheRequestComplete()
                completed_requests.append( cache_request )
        for completed_request in completed_requests:
            del self.PendingTask[ completed_request ]

    def findCachedDomain(self, var_cache_id, region, var_specs ):
        cached_var = self.CachedVariables.setdefault( var_cache_id, CachedVariable( id=var_cache_id, specs=var_specs) )
        if ( cached_var is not None ):
            overlap,domain = cached_var.findDomain( region )
            if overlap == Domain.CONTAINED: return cached_var, domain
        return cached_var, None

    def getNextWorkerQueue( self ):
        wqueue = "wq%d" % self.WorkerQueueIndex
        self.WorkerQueueIndex = (self.WorkerQueueIndex + 1) % self.NumWorkers
        return wqueue

    def execute( self, run_args ):
        debug = True
        designated_worker = None
        wpsLog.info( "Executing Celery engine, args: %s" % ( run_args ) )
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
                cache_op_args = { 'data':var_mdata } if operation else { 'region':region, 'data':var_mdata }
                worker_id = self.getNextWorkerQueue()
                cache_request = execute.apply_async( [ cache_op_args ] ) # , queue=worker_id )
                cached_domain = cached_var.addDomain( region, queue=worker_id )
                self.PendingTasks[ cache_request ] = cached_domain
            else:
                worker_id, cache_request_status = cached_domain.getCacheStatus()
                if cache_request_status == Domain.COMPLETE:
                    designated_worker = worker_id

        if operation:
            if designated_worker is None:
                task = execute.delay( run_args )
            else:
                task = execute.apply_async( [ run_args ], queue=designated_worker )

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
            return result


if __name__ == "__main__":
    import sys, pprint
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)

    variable =  { 'collection': 'MERRA/mon/atmos', 'id': 'clt' }

    region1    = { "longitude":-24.20, "latitude":58.45 }
    region2    = { "longitude":-30.20, "latitude":67.45 }
    cache_region    = { "longitude": [ -60.0, 0.0 ], "latitude": [ 30.0, 90.0 ] }

    op_annual_cycle =  {"kernel":"time", "type":"climatology", "bounds":"annualcycle"}
    op_departures =  {"kernel":"time", "type":"departures",  "bounds":"np"}


    engine = CeleryEngine('celery')
    run_args = { 'data': variable, 'region':region1, 'operation': op_departures }
    result = engine.execute( run_args )

    print "\n ---------- Result: ---------- "
    pp.pprint(result)
