import unittest, json
from request.manager import TaskRequest
from engines import engineRegistry
from datacache.domains import Domain, Region
from modules.configuration import MERRA_TEST_VARIABLES, CDAS_COMPUTE_ENGINE

class EngineTests(unittest.TestCase):

    def setUp(self):
        self.test_point = [ -137.0, 35.0, 85000 ]
        self.test_time = '2010-01-16T12:00:00'
        self.operations = [ "time.departures(v0,slice:t)", "time.climatology(v0,slice:t,bounds:annualcycle)", "time.value(v0)" ]
        self.def_task_args =  { 'region': self.getRegion(), 'data': self.getData() }
        self.engine = engineRegistry.getInstance( CDAS_COMPUTE_ENGINE + "Engine" )
        self.cache_region = { "level": 85000 }

    def tearDown(self):
        pass

    def getRegion(self):
        return '{"longitude": %.2f, "latitude": %.2f, "level": %.2f, "time":"%s" }' % (self.test_point[0],self.test_point[1],self.test_point[2],self.test_time)

    def getData(self, vars=[0]):
        var_list = ','.join( [ ( '"v%d:%s"' % ( ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '{"%s":[%s]}' % ( MERRA_TEST_VARIABLES["collection"], var_list )
        return data

    def getOp(self, op_index ):
        return [ self.operations[ op_index ] ]

    def getTaskArgs(self, op ):
        task_args = dict( self.def_task_args )
        task_args['operation'] = op
        return task_args

    def getResultData( self, results, index=0 ):
        if isinstance( results, Exception ):
            self.fail(str(results))
        return results[index]['data']

    def getResultStats( self, results, index=0 ):
        from modules.utilities import ExecutionRecord
        if isinstance( results, Exception ):
            self.fail(str(results))
        return ExecutionRecord( results[index]['exerec'] )

    def assertStatusEquals(self, result, **kwargs ):
        status = self.getResultStats( result )
        for item in kwargs.iteritems():
            self.assertEqual( status[item[0]], item[1] )

    def xtest01_cache(self):
        result = self.engine.execute( TaskRequest( request={ 'region': self.cache_region, 'data': self.getData() } ) )
        self.assertStatusEquals( result, cache_add=self.cache_region )

    def test02_departures(self):
        test_result = [  -1.405364990234375, -1.258880615234375, 0.840728759765625, 2.891510009765625, -18.592864990234375,
                        -11.854583740234375, -3.212005615234375, -5.311614990234375, 5.332916259765625, -1.698333740234375,
                          8.750885009765625, 11.778228759765625, 12.852447509765625 ]
        task_args = self.getTaskArgs( op=self.getOp( 0 ) )
        result = self.engine.execute( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result )
        compute_result = result_data[0:len(test_result)]
        self.assertEqual( test_result, compute_result )
        self.assertStatusEquals( result, cache_found=Domain.COMPLETE, cache_found_domain=self.cache_region,  designated=True )

    def xtest03_annual_cycle(self):
        test_result = [48.07984754774306, 49.218166775173614, 49.36114501953125, 46.40715196397569, 46.3406982421875, 44.37486775716146, 46.54383680555556, 48.780619303385414, 46.378028021918404, 46.693325466579864, 48.840003119574654, 46.627953423394096]
        task_args = self.getTaskArgs( op=self.getOp( 1 ) )
        result = self.engine.execute( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result )
        self.assertEqual( test_result, result_data[0:len(test_result)] )

    def xtest04_value_retreval(self):
        test_result = 59.765625
        task_args = self.getTaskArgs( op=self.getOp( 2 ) )
        result = self.engine.execute( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result )
        self.assertEqual( test_result, result_data )

    def xtest05_multitask(self):
        test_results = [ [ -1.405364990234375, -1.258880615234375, 0.840728759765625 ], [48.07984754774306, 49.218166775173614, 49.36114501953125], 59.765625 ]
        task_args = self.getTaskArgs( op=self.operations )
        results = self.engine.execute( TaskRequest( request=task_args ) )
        for ir, result in enumerate(results):
            result_data = self.getResultData( results, ir )
            test_result = test_results[ir]
            if hasattr( test_result, '__iter__' ):  self.assertEqual( test_result, result_data[0:len(test_result)] )
            else:                                   self.assertEqual( test_result, result_data )


if __name__ == '__main__':
    test_runner = unittest.TextTestRunner(verbosity=2)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase( EngineTests )
    test_runner.run( suite )

