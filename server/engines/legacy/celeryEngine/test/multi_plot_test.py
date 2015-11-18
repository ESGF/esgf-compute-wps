import time
from modules.utilities import *
from engines.celery.celeryEngine import CeleryEngine

def run_test():
    import sys, pprint
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)

    variable =  { 'collection': 'MERRA/mon/atmos', 'id': 'clt' }

    region1    = { "longitude": -24.20, "latitude": 58.45, "level": 8000.0 }
    region2    = { "longitude": -30.20, "latitude": 67.45, "level": 8000.0 }

    op_annual_cycle =  {"kernel":"time", "type":"climatology", "bounds":"annualcycle"}
    op_departures =  {"kernel":"time", "type":"departures",  "bounds":"np"}

    engine = CeleryEngine('celery')

    t0 = time.time()
    run_args = { 'data': variable, 'region': region1, 'operation': [ op_departures, op_annual_cycle ] }
    task = engine.execute( run_args, async=True )

    t1 = time.time()

    result = task.get() if task else None
    print "\n ---------- Results: ---------- "
    pp.pprint(result) if result else "<NONE>"
    t2 = time.time()

    run_args = { 'data': variable, 'region': region2, 'operation': [ op_departures, op_annual_cycle ] }
    task = engine.execute( run_args, async=True )

    result = task.get() if task else None
    print "\n ---------- Results: ---------- "
    pp.pprint(result) if result else "<NONE>"
    t3 = time.time()

    print "\n Operations Complete, dt0 = %.2f, dt1 = %.2f, dt2 = %.2f, dt = %.2f  " % ( t1-t0, t2-t1, t3-t2, t3-t0 )

if __name__ == "__main__":
#    import cProfile
#    cProfile.run( 'run_test()', None, 'time' )

    run_test()



