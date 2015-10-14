from modules.utilities import *

def wrank( value ):
    return ( value if not isinstance(value,basestring) else int( value.split('-')[1] ) )

def wid( value ):
    return ( value if not isinstance(value,int) else 'W-%d' % value )


class TaskMonitor:

    Monitors = {}

    def __init__( self, rid, **kwargs ):
        self._request_id = rid
        self.Monitors[rid] = self

    def __str__(self):
        return "TaskMonitor[%s:%x]" % ( self._request_id, id(self) )

    @property
    def rid(self):
        return self._request_id

    @classmethod
    def get_monitor( cls, rid ):
        return cls.Monitors.get( rid, None )

    def status(self):
        raise Exception( 'Error: status method not implemented in TaskMonitor')

    def taskName(self):
        raise Exception( 'Error: taskName method not implemented in TaskMonitor')

    def ready(self):
        raise Exception( 'Error: ready method not implemented in TaskMonitor')

    def result(self,**args):
        raise Exception( 'Error: result method not implemented in TaskMonitor')

    def addStats(self,**args):
        raise Exception( 'Error: addStats method not implemented in TaskMonitor')


class ComputeEngineCommunicator:

    WS_FREE = 1
    WS_OP = 2
    WS_CACHE = 3

    def __init__( self ):
        self.worker_stats = None

    def submitTaskImpl( self, task_request, worker ):
        raise Exception( 'Error: submitTask method not implemented in engine communicator' )

    def close(self):
        raise Exception( 'Error: close method not implemented in engine communicator' )

    def submitTask( self, task_request, worker ):
        task_monitor = self.submitTaskImpl( task_request, worker )
        if task_request['utility'] is None:
     #       task_type = 'CACHE' if task_request.isCacheOp() else "OP"
     #       wpsLog.error( "\nCComputeEngineCommunicator:submitTask-> type:%s, w=%s, rid=%s, t=%.2f\n" % ( task_type, worker, task_monitor.rid, time.time()%1000.0  ) )
            if task_request.isCacheOp():
                self.setWorkerState( worker, self.WS_CACHE )
            else:
                self.setWorkerState( worker, self.WS_OP )
        else:
            wpsLog.error( "\nCComputeEngineCommunicator:submitTask-> type: UTIL, w=%s, rid=%s, t=%.2f\n" % ( worker, task_monitor.rid, time.time()%1000.0  ) )
        return task_monitor

    def getWorkerStats(self):
        self.updateWorkerStats()
        return self.worker_stats

    def setWorkerState( self, widspec, state ):
        if widspec == "*":
            workers = self.worker_stats.keys()
        elif isinstance( widspec, list ):
            workers = widspec
        else:
            workers = [ widspec ]
        for wid in workers:
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
        if not self.worker_stats:
            t0 = time.time()
            self.worker_stats = self.initWorkerStats()
            t1 = time.time()
            wpsLog.debug( " ***** updateWorkerStats[ dt = %0.3f ], workers: %s" %  ( t1-t0, str(self.worker_stats.keys()) ) )
        if len( self.worker_stats ) == 0:
            wpsLog.error( "ERROR: Must start up workers!" )

    def getNextWorker( self, is_cache=False ):
        mdbg = False
        if mdbg: wpsLog.debug( "  %%%%%%%%%%%%%%%% GetNextWorker: ")
        operational_worker = None
        free_worker = None
        op_worker_tstamp = float('Inf')
        free_worker_cache_size = float('Inf')
        for worker, wstat in self.worker_stats.items():
            wstate =  wstat.get('state', self.WS_FREE )
            if mdbg: wpsLog.debug( "  >>-----> Worker: '%s', wstat: %s " % ( worker, str(wstat) ) )
            if wstate == self.WS_FREE:
                cache_size =  wstat.setdefault( 'csize', 0 )
                if(cache_size < free_worker_cache_size):
                   free_worker =  worker
                   free_worker_cache_size = cache_size
            elif wstate == self.WS_OP:
                ts = wstat['tstamp']
                if(ts < op_worker_tstamp):
                   operational_worker =  worker
                   op_worker_tstamp = ts
        if mdbg: wpsLog.debug( "  SELECTED Free Worker: %s, Operational Worker: %s " % ( free_worker, operational_worker) )
        if free_worker is not None:
            # if is_cache:
            #     wstat = self.worker_stats[free_worker]
            #     wstat['csize'] =  free_worker_cache_size + 1
            return free_worker
        else:
            return operational_worker

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

    @staticmethod
    def kill_all_zombies():
    #                                              Cleanup abandoned processes
        import subprocess, signal
        proc_specs = subprocess.check_output('ps').split('\n')
        for proc_spec in proc_specs:
            if ('pydev' in proc_spec) or ('utrunner' in proc_spec) or ('WPCDAS' in proc_spec) or ('manage.py' in proc_spec):
                pid = int( proc_spec.split()[0] )
                if pid <> os.getpid():
                    os.kill( pid, signal.SIGKILL )
                    print "Killing proc: ", proc_spec


if __name__ == '__main__':
    ComputeEngineCommunicator.kill_all_zombies()