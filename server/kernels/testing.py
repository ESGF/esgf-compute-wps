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
        self.ave_operations = [ "CWT.average(*,axis:z)", "CWT.average(*,axis:ze)" ]
        self.def_task_args =  { 'domain': self.getRegion(), 'variable': self.getData() }

    def tearDown(self):
        pass

    def getRegion(self, ipt=0 ):
        return '[{"id":"r0","longitude": {"value":%.2f,"system":"values"}, "latitude": %.2f, "level": %.2f, "time":"%s" }]' % (self.test_point[0]+5*ipt,self.test_point[1]-5*ipt,self.test_point[2],self.test_time)

    def getData(self, vars=[0]):
        var_list = ','.join( [ ( '{"dset":"%s","id":"v%d:%s","domain":"r0"}' % ( MERRA_TEST_VARIABLES["collection"], ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '[%s]' % ( var_list )
        return data

    def getEnsembleData(self,var="ta"):
        collections = [ "MERRA/mon/atmos", "CFSR/mon/atmos" ]
        var_list = ','.join( [ ( '{"dset":"%s","id":"v%d:%s","domain":"r0"}' % ( collections[ivar], ivar, var ) ) for ivar in range(len(collections)) ] )
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

    def getResultData( self, response, rindex=0 ):
        results = self.getResults( response )
        rdata = results[rindex].get('data',[])
        return ( [ float(rd) for rd in rdata ] if hasattr( rdata, '__iter__' ) else float(rdata) )


    def getTaskArgs(self, op, ipt=0 ):
        task_args = { 'domain': self.getRegion(ipt), 'variable': self.getData(), 'embedded': True }
        task_args['operation'] = op
        return task_args

    def test01_cache(self):
        cache_level = 85000.0
        request_region = Region( { "lev": {"config":{},"bounds":[cache_level]}, "id":"r0" } )
        results = self.getResults( kernelMgr.run( TaskRequest( request={ 'domain': [ {"id":"r0", "level": cache_level } ], 'data': self.getData() } ) ) )
        result_stats = results[0][0]
        cached_region = result_stats['region']
        self.assertEqual(cached_region, request_region )

    def test02_departures(self):
        test_result = [  -1.405364990234375, -1.258880615234375, 0.840728759765625, 2.891510009765625, -18.592864990234375,
                        -11.854583740234375, -3.212005615234375, -5.311614990234375, 5.332916259765625, -1.698333740234375,
                          8.750885009765625, 11.778228759765625, 12.852447509765625 ]
        task_args = self.getTaskArgs( op=self.getOp( 0 ) )
        result_data = self.getResultData( kernelMgr.run( TaskRequest( request=task_args ) ) )
        self.assertEqual( test_result, result_data[0:len(test_result)] )


    def test03_annual_cycle(self):
        test_result = [38.20164659288194, 40.60203721788194, 39.744038899739586, 37.738803439670136,
                       34.758260091145836, 32.372643364800346, 33.70814344618056, 35.980190700954864,
                       37.239708794487846, 38.93236626519097, 39.45347425672743, 35.83015611436632]
        task_args = self.getTaskArgs( self.getOp( 1 ), 1 )
        kernelMgr.persist()
        result_data = self.getResultData( kernelMgr.run( TaskRequest( request=task_args ) ) )
        self.assertEqual( test_result, result_data[0:len(test_result)] )

    def test04_value_retreval(self):
        test_result = 28.41796875
        task_args = self.getTaskArgs( self.getOp( 2 ), 2 )
        result_data =  self.getResultData( kernelMgr.run( TaskRequest( request=task_args ) ) )
        self.assertEqual( test_result, result_data )

    def test05_multitask(self):
        test_results = [ [ -1.405364990234375, -1.258880615234375, 0.840728759765625 ], [ 48.07984754774306, 49.218166775173614, 49.36114501953125 ], 59.765625 ]
        task_args = self.getTaskArgs( op=self.operations )
        results = kernelMgr.run( TaskRequest( request=task_args ) )
        for ir, test_result in enumerate( test_results ):
            result = self.getResultData( results, ir )
            if hasattr( test_result, '__iter__' ):  self.assertEqual( test_result, result[0:len(test_result)] )
            else:                                   self.assertEqual( test_result, result )

    def test06_average(self):
        test_result = [60.45402434065925, 59.35986168551339, 59.17586797470401, 58.17904816915278, 58.354240161557236, 58.90145082179115, 59.25474219650481 ]
        op_domain = Region( { "lev": {"config":{},"bounds":[85000.0]}, "id":"r0" } )
        task_args = { 'domain': op_domain, 'variable': self.getData(), 'operation' : [ "CWT.average(*,axis:xy)" ],'embedded': True }
        result_data = self.getResultData( kernelMgr.run( TaskRequest( request=task_args ) ) )
        self.assertEqual( test_result, result_data[0:len(test_result)] )

    def test07_ensemble_average(self):
        test_result = [ 279.6803316230488, 279.7325255033889, 280.32625029619817, 280.9536142220719, 281.875403712933, 282.6501879774603, 282.9003404482816, 282.7242298368891, 282.048408536275 ]
        op_domain = Region( { "lev": {"config":{},"bounds":[85000.0]}, "id":"r0" } )
        task_args = { 'domain': op_domain, 'variable': self.getEnsembleData(), 'operation' : [ "CWT.average(*,axis:xye)" ],'embedded': True }
        result_data = self.getResultData( kernelMgr.run( TaskRequest( request=task_args ) ) )
        self.assertEqual( test_result, result_data[0:len(test_result)] )
