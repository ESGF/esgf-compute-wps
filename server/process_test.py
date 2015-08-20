import sys
import time
import logging
import pprint

from modules.utilities import wpsLog
from kernels.timeseries_analysis import TimeseriesAnalytics


if __name__ == "__main__":
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)

    variables = [ { 'collection': 'MERRA/mon/atmos', 'id': 'clt' },
                  { 'url': 'file://usr/local/web/data/MERRA/u750/merra_u750.xml', 'id': 'u' },
                  { 'url': 'file://usr/local/web/data/MERRA/MERRA100.xml', 'id': 't' },
                  { 'url': 'file://usr/local/web/data/MERRA/u750/merra_u750.nc', 'id': 'u' },
                  { 'url': 'file://usr/local/web/data/MERRA/u750/merra_u750_1979_1982.nc', 'id': 'u' },
                  { 'url': 'file://usr/local/web/WPCDAS/data/TestData.nc', 'id': 't' },
                  { 'url': 'file://usr/local/web/WPCDAS/data/atmos_ua.nc', 'id': 'ua' } ]
    var_index = 0
    region1    = { "longitude":-24.20, "latitude":58.45 }
    region2    = { "longitude":-30.20, "latitude":67.45 }
    cache_region    = { "longitude": [ -60.0, 0.0 ], "latitude": [ 30.0, 90.0 ] }

    operations = [ '{"kernel":"time", "type":"climatology", "bounds":"annualcycle"}',
                   '{"kernel":"time", "type":"departures",  "bounds":"np"}',
                   { 'type': 'climatology', 'bounds': 'annualcycle' },
                   { 'type': 'departures', 'bounds': '' },
                   { 'type': 'climatology', 'bounds': '' },
                   { 'type': 'departures', 'bounds': 'np' },
                   { 'type': 'climatology', 'bounds': 'np' },
                   { 'type': '', 'bounds': '' } ]
    operation_index = 1

    processor = TimeseriesAnalytics( cache=True )

    t0 = time.time()
    result = processor.run( { 'data':variables[var_index], 'region': cache_region } )
    t1 = time.time()
    print "\n\n ------ Cache execution TIME: %.2f -------- \n\n" % ( t1-t0 )
    print "\n ---------- Result: ---------- "
    pp.pprint(result)


    t0 = time.time()
    result = processor.run( { 'data':variables[var_index], 'region': region1, 'operation':operations[operation_index]} )
    t1 = time.time()
    print "\n\n ------ First execution TIME: %.2f -------- \n\n" % ( t1-t0 )
    print "\n ---------- Result: ---------- "
    pp.pprint(result)

    t0 = time.time()
    result = processor.run( { 'data':variables[var_index], 'region': region2, 'operation':operations[operation_index] } )
    t1 = time.time()
    print "\n\n ------ Second execution TIME: %.2f -------- \n\n" % ( t1-t0 )
    print "\n ---------- Result: ---------- "
    pp.pprint(result)

