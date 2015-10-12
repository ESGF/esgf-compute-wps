import unittest, json, sys, logging
from request.manager import TaskRequest
from manager import KernelManager
from modules.configuration import MERRA_TEST_VARIABLES
from modules.utilities import wpsLog
from datacache.domains import Region
verbose = False

if verbose:
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)

kernelMgr = KernelManager('W-1')

class KernelTests(unittest.TestCase):

    def setUp(self):
        self.test_point = [ -137.0, 35.0, 85000.0 ]
        self.test_time = '2010-01-16T12:00:00'
        self.operations = [ "CDTime.departures(v0,slice:t)", "CDTime.climatology(v0,slice:t,bounds:annualcycle)", "CDTime.value(v0)" ]
        self.def_task_args =  { 'domain': self.getRegion(), 'variable': self.getData() }

    def tearDown(self):
        pass

    def getRegion(self, ipt=0 ):
        return '[{"id":"r0","longitude": {"value":%.2f,"system":"value"}, "latitude": %.2f, "level": %.2f, "time":"%s" }]' % (self.test_point[0]+5*ipt,self.test_point[1]-5*ipt,self.test_point[2],self.test_time)

    def getData(self, vars=[0]):
        var_list = ','.join( [ ( '{"dset":"%s","id":"v%d:%s","domain":"r0"}' % ( MERRA_TEST_VARIABLES["collection"], ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '[%s]' % ( var_list )
        return data

    def getLocalData(self):
        data = '{"dset":"MERRA/mon/atmos/hur","id":"v0:hur","domain":"r0"}'
        return data

    def getOp(self, op_index ):
        return [ self.operations[ op_index ] ]

    def getResults( self, response ):
        error = response.get( 'error', None )
        if error: raise Exception( error )
        return response['results']

    def getTaskArgs(self, op, ipt=0 ):
        task_args = { 'domain': self.getRegion(ipt), 'variable': self.getData() }
        task_args['operation'] = op
        return task_args

    def test01_cache(self):
        cache_level = 85000.0
        results = self.getResults( kernelMgr.run( TaskRequest( request={ 'domain': [ {"id":"r0", "level": cache_level } ], 'data': self.getData() } ) ) )
        result_stats = results[0]['result'][0]
        cached_region = Region( json.loads(result_stats['region']) )
        request_region = Region( { "lev": {"config":{},"bounds":[cache_level]} } )
        test = (cached_region == request_region)
        self.assertEqual(cached_region, request_region )

    def test02_departures(self):
        test_result = [  -1.405364990234375, -1.258880615234375, 0.840728759765625, 2.891510009765625, -18.592864990234375,
                        -11.854583740234375, -3.212005615234375, -5.311614990234375, 5.332916259765625, -1.698333740234375,
                          8.750885009765625, 11.778228759765625, 12.852447509765625 ]
        task_args = self.getTaskArgs( op=self.getOp( 0 ) )
        results = self.getResults( kernelMgr.run( TaskRequest( request=task_args ) ) )
        result_data = results[0].get('data',[])
        self.assertEqual( test_result, result_data[0:len(test_result)] )

    def test03_annual_cycle(self):
        test_result = [38.20164659288194, 40.60203721788194, 39.744038899739586, 37.738803439670136,
                       34.758260091145836, 32.372643364800346, 33.70814344618056, 35.980190700954864,
                       37.239708794487846, 38.93236626519097, 39.45347425672743, 35.83015611436632]
        task_args = self.getTaskArgs( self.getOp( 1 ), 1 )
        kernelMgr.persist()
        results = self.getResults( kernelMgr.run( TaskRequest( request=task_args ) ) )
        result_data = results[0].get('data',[])
        self.assertEqual( test_result, result_data[0:len(test_result)] )

    def xtest04_value_retreval(self):
        test_result = 28.41796875
        task_args = self.getTaskArgs( self.getOp( 2 ), 2 )
        results =  self.getResults( kernelMgr.run( TaskRequest( request=task_args ) ) )
        result_data = results[0].get('data',[])
        self.assertEqual( test_result, result_data )

    def xtest05_multitask(self):
        test_results = [ [ -1.405364990234375, -1.258880615234375, 0.840728759765625 ], [ 48.07984754774306, 49.218166775173614, 49.36114501953125 ], 59.765625 ]
        task_args = self.getTaskArgs( op=self.operations )
        results = self.getResults( kernelMgr.run( TaskRequest( request=task_args ) ) )
        for ir, result in enumerate(results):
            result_data = result.get('data',[])
            test_result = test_results[ir]
            if hasattr( test_result, '__iter__' ):  self.assertEqual( test_result, result_data[0:len(test_result)] )
            else:                                   self.assertEqual( test_result, result_data )

    def xtest06_stats(self):
        results = kernelMgr.run( TaskRequest( utility='worker.cache' ) )
        print results
