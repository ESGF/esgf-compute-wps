from modules.utilities import *

def run_test():
    from engines.celery.celeryEngine import CeleryEngine
    from request.manager import TaskRequest
    import sys, pprint, time

    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)

    variable =  { 'collection': 'MERRA/mon/atmos', 'id': 'hur' }
    region    = { "longitude": -137.09, "latitude": 35.4876, "level": 85000.0, "time": "1979-1-16 12:0:0.0" }

    op_annual_cycle =  {"kernel":"time", "type":"climatology", "bounds":"annualcycle"}
    op_departures =  {"kernel":"time", "type":"departures",  "bounds":"np"}
    op_value =  {"kernel":"time", "type":"value" }

    engine = CeleryEngine('celery')

    t0 = time.time()
    request = TaskRequest( task= { 'data': variable, 'region': region, 'operation': [ op_value ] } )
    task = engine.execute( request, async=True )

    t1 = time.time()

    result = task.get() if task else None
    print "\n ---------- Result: ---------- "
    pp.pprint(result) if result else "<NONE>"
    t2 = time.time()

    print "\n Operation Complete, dt0 = %.2f, dt1 = %.2f, dt = %.2f  " % ( t1-t0, t2-t1, t2-t0 )

if __name__ == "__main__":

    run_test()



