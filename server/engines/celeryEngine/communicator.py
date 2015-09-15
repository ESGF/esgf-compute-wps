from engines.communicator import ComputeEngineCommunicator, TaskMonitor
from tasks import execute

class CeleryTaskMonitor(TaskMonitor):

    def __init__( self, rid, **args ):
        TaskMonitor. __init__( self, rid, **args )
        self.task = args.get('task',None)
        self.stats = {}

    def status(self):
        return self.task.status

    def ready(self):
        return self.task.ready()

    def result( self, **args ):
        self.addStats( **args )
        response = self.task.get()
        results = response['results']
        if len( self.stats ):
            for result in results: result.update( self.stats )
        return results

    def taskName(self):
        return self.task.task_name

    def addStats(self,**args):
        self.stats.update( args )

class CeleryCommunicator( ComputeEngineCommunicator ):

    def __init__( self ):
        ComputeEngineCommunicator.__init__( self )


    def submitTaskImpl( self, task_request, worker ):
        task = execute.apply_async( (task_request.task,), exchange='C.dq', routing_key=worker )
        return CeleryTaskMonitor( task.id, task=task )

    def getWorkerStats(self):
        import celery
        return celery.current_app.control.inspect().stats()


def run_test():
    import pprint, time, logging, sys
    from modules.utilities import wpsLog
    from tasks import CeleryEngine
    from request.manager import TaskRequest
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)
    test_cache = False

    variable =   { 'collection': 'MERRA/mon/atmos', 'id': 'clt' }

    region1    = { "longitude":-24.20, "latitude":58.45, "level": 10000.0 }
    region2    = { "longitude":-30.20, "latitude":67.45, "level": 8500.0 }
    op_annual_cycle =   {"kernel":"time", "type":"climatology", "bounds":"annualcycle"}
    op_departures =   {"kernel":"time", "type":"departures",  "bounds":"np"}

    engine = CeleryEngine('celery')

    if test_cache:
        request = TaskRequest( task={ 'data': variable } )
        task = engine.execute( request, async=True )
        result = task.result()
        print "\n ---------- Result(cache): ---------- "
        pp.pprint(result)
    else:
        run_async = True
        if run_async:
            t0 = time.time()
            request = TaskRequest( task={ 'data': variable, 'region':region1, 'operation': op_departures } )
            task1 = engine.execute( request, async=True )

#            request = TaskRequest( task={ 'data': variable, 'region':region2, 'operation': op_annual_cycle } )
#            task2 = engine.execute( request, async=True )

            t1 = time.time()

            result1 = task1.result() if task1 else None
            print "\n ---------- Result (departures): ---------- "
            pp.pprint(result1) if result1 else "<NONE>"
            t2 = time.time()

#            print "\n ---------- Result (annual cycle): ---------- "
#            result2 = task2.result() if task2 else None
#            pp.pprint(result2) if result2 else "<NONE>"
#            t3 = time.time()
#            print "\n Operations Complete, dt0 = %.2f, dt1 = %.2f, , dt2 = %.2f, dt = %.2f  " % ( t1-t0, t2-t1, t3-t2, t3-t0 )
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
    run_test()


