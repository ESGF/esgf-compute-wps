from modules.utilities import *

class TaskMonitor:

    def __init__( self, id, **kwargs ):
        self._id = id

    @property
    def id(self):
        return self._id

    def status(self):
        raise Exception( 'Error: status method not implemented in TaskMonitor')

    def taskName(self):
        raise Exception( 'Error: taskName method not implemented in TaskMonitor')

    def ready(self):
        raise Exception( 'Error: ready method not implemented in TaskMonitor')

    def result(self,**args):
        raise Exception( 'Error: result method not implemented in TaskMonitor')

class ComputeEngineCommunicator:

    WS_FREE = 1
    WS_OP = 2
    WS_CACHE = 3

    def __init__( self ):
        self.worker_stats = {}

    def submitTaskImpl( self, task_request, worker ):
        raise Exception( 'Error: submitTask method not implemented in engine communicator')

    def getWorkerStatsImpl(self):
        raise Exception( 'Error: getWorkerStats method not implemented in engine communicator')

    def submitTask( self, task_request, worker ):
        self.submitTask( task_request, worker )
        if task_request.isCacheOp():    self.setWorkerState( worker, self.WS_CACHE )
        else:                           self.setWorkerState( worker, self.WS_OP )

    def getWorkerStats(self):
        return self.getWorkerStatsImpl()

    def setWorkerState( self, wid, state ):
        wstat = self.worker_stats.get( wid, None )
        if ( wstat is None ):
            wpsLog.error( "Unrecognized worker ID in 'setWorkerState[%d]': %s" % ( state, wid ) )
        else:
            wstat['state'] = state
            wstat['tstamp'] = time.time()
        if state == self.WS_CACHE:
            wstat['csize'] = wstat.get('csize', 0 ) + 1

    def clearWorkerState( self, worker ):
        self.setWorkerState( worker, self.WS_FREE )

    def getWorkerState( self, wid ):
        wstat = self.worker_stats.get( wid, None )
        if ( wstat is None ):
            wpsLog.error( "Unrecognized worker ID in 'getWorkerState': %s" % ( wid ) )
            return None
        else:
            return wstat.get('state', self.WS_FREE )

    def updateWorkerStats(self):
        t0 = time.time()
        if len( self.worker_stats ) == 0:
            self.worker_stats = self.getWorkerStats()
        if self.worker_stats == None:
            wpsLog.error( "ERROR: Must start up workers!" )
            self.worker_stats = {}
        t1 = time.time()
        wpsLog.debug( " ***** updateWorkerStats[ dt = %0.3f ], workers: %s" %  ( t1-t0, str(self.worker_stats.keys()) ) )


    def getNextWorker( self ):
        mdbg = False
        if mdbg: wpsLog.debug( "  %%%%%%%%%%%%%%%% GetNextWorker: ")
        operational_worker = None
        free_worker = None
        op_worker_tstamp = float('Inf')
        free_worker_cache_size = float('Inf')
        for worker, wstat in self.worker_stats.items():
            wstate =  wstat.get('state', self.WS_FREE )
            if mdbg: wpsLog.debug( "  >>-----> Worker: '%s', state: %d " % ( worker, wstate) )
            if wstate == self.WS_FREE:
                cache_size =  wstat.get( 'csize', 0 )
                if(cache_size < free_worker_cache_size):
                   free_worker =  worker
                   free_worker_cache_size = cache_size
            elif wstate == self.WS_OP:
                ts = wstat['tstamp']
                if(ts < op_worker_tstamp):
                   operational_worker =  worker
                   op_worker_tstamp = ts
        return free_worker if free_worker is not None else operational_worker

    # def updateWorkerCacheSize( self, wid, additional_cache_size ):
    #     wstat = self.worker_stats.get( wid, None )
    #     if ( wstat is None ):
    #         wpsLog.error( "Unrecognized worker ID in 'updateWorkerCacheSize': %s" % ( wid ) )
    #     else:
    #         wstat['csize'] = wstat.get('csize', 0 ) + additional_cache_size
    #
    # def getWorkerCacheSize( self, wid ):
    #     wstat = self.worker_stats.get( wid, None )
    #     if ( wstat is None ):
    #         wpsLog.error( "Unrecognized worker ID in 'getWorkerCacheSize': %s" % ( wid ) )
    #         return None
    #     else:
    #         return wstat.get('csize', 0 )
