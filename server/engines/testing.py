import unittest, json
from request.manager import TaskRequest
from engines import engineRegistry
from datacache.domains import Domain, Region
from modules.configuration import MERRA_TEST_VARIABLES, CDAS_COMPUTE_ENGINE
from modules.utilities import *

class EngineTests(unittest.TestCase):

    def setUp(self):
        self.test_point = [ -137.0, 35.0, 85000 ]
        self.test_time = '2010-01-16T12:00:00'
        self.operations = [ "CDTime.departures(v0,slice:t)", "CDTime.climatology(v0,slice:t,bounds:annualcycle)", "CDTime.value(v0)" ]
        self.indexed_operations = [ "CDTime.departures($0,slice:t)", "CDTime.climatology($0,slice:t,bounds:annualcycle)", "CDTime.value($0)" ]
        self.def_task_args =  { "domain": self.getRegion(), "variable": self.getData(), 'embedded': True, 'async': False }
        self.engine = engineRegistry.getInstance( CDAS_COMPUTE_ENGINE + "Engine" )
        self.cache_region =  Region( { "lev": {"config":{},"bounds":[ self.test_point[2] ], 'id':"r0" } } )

    def tearDown(self):
        pass

    def getRegion(self, ipt=0 ):
        return '[{"id":"r0","longitude": {"value":%.2f,"system":"values"}, "latitude": %.2f, "level": %.2f, "time":"%s" }]' % (self.test_point[0]+5*ipt,self.test_point[1]-5*ipt,self.test_point[2],self.test_time)

    def getData(self, vars=[0]):
        var_list = ','.join( [ ( '{"dset":"%s","id":"v%d:%s","domain":"r0"}' % ( MERRA_TEST_VARIABLES["collection"], ivar, MERRA_TEST_VARIABLES["vars"][ivar] ) ) for ivar in vars ] )
        data = '[%s]' % ( var_list )
        return data

    def getLocalData(self):
        data = '{"dset":"MERRA/mon/atmos/hur","id":"v0:hur","domain":"r0"}'
        return data

    def getOp(self, op_index ):
        return [ self.operations[ op_index ] ]

    def getTaskArgs(self, op ):
        task_args = dict( self.def_task_args )
        task_args['operation'] = op
        return task_args

    def getResultData( self, results, index=0 ):
        if isinstance( results, Exception ):
            raise results
        if isinstance( results, list ): rdata = results[index].get( 'data', None )
        else:                           rdata = results.get( 'data', None )
        return None if (rdata is None) else ( [ float(rd) for rd in rdata ] if hasattr( rdata, '__iter__' ) else float(rdata) )

    def getResultStats( self, results, index=0 ):
        if isinstance( results, Exception ):
            self.fail(str(results))
        return ExecutionRecord( results[index]['exerec'] )

    def assertStatusEquals(self, result, **kwargs ):
        status = self.getResultStats( result )
        for item in kwargs.iteritems():
            self.assertEqual( status[item[0]], item[1] )

    def test010_cache(self):
        result = self.engine.execute( TaskRequest( request={ 'domain': self.cache_region, 'variable': self.getData(), 'async': False } ) )
        self.assertEqual( result['cache_region'], self.cache_region )
        wpsLog.debug( "\n\n ++++++++++++++++ ++++++++++++++++ ++++++++++++++++ Cache Result: %s\n\n ", str(result ) )
        if CDAS_COMPUTE_ENGINE == 'mpi':
            cached_var, domain = self.engine.findCachedDomain( result['cached_var'], self.cache_region )
            source = result['cache_worker']
            srank = wrank(source)
            dest = 'W-1' if (srank == 0) else 'W-0'
            task_args = { 'source': source, 'destination': dest, 'domain_spec': domain.getDomainSpec(), 'async': False, 'embedded': True  }
            t0 = time.time()
            results = self.engine.execute( TaskRequest( utility='domain.transfer', request=task_args ) )
            t1 = time.time()
            wpsLog.debug( "\n\n ++++++++++++++++ ++++++++++++++++ ++++++++++++++++ Transfer (dt = %0.2f) Results: %s\n\n " % (t1-t0, str(results) ) )
            transferred_shape = domain.variable_spec['shape']
            worker_results = [ worker_response['results'] for worker_response in results ]
            for worker_result in worker_results:
                if worker_result and (worker_result[0] in ["source","destination"]):
                    self.assertSequenceEqual( transferred_shape, worker_result[2] )

    def test02_departures(self):
        test_result = [  -1.405364990234375, -1.258880615234375, 0.840728759765625, 2.891510009765625, -18.592864990234375,
                        -11.854583740234375, -3.212005615234375, -5.311614990234375, 5.332916259765625, -1.698333740234375,
                          8.750885009765625, 11.778228759765625, 12.852447509765625 ]
        task_args = self.getTaskArgs( op=[ self.indexed_operations[0] ] )
        result = self.engine.execute( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result )
        compute_result = result_data[0:len(test_result)]
        self.assertEqual( test_result, compute_result )
#        self.assertStatusEquals( result, cache_found=Domain.COMPLETE, cache_found_domain=self.cache_region,  designated=True )

    def test03_annual_cycle(self):
        test_result = [48.07984754774306, 49.218166775173614, 49.36114501953125, 46.40715196397569, 46.3406982421875, 44.37486775716146, 46.54383680555556, 48.780619303385414, 46.378028021918404, 46.693325466579864, 48.840003119574654, 46.627953423394096]
        task_args = self.getTaskArgs( op=self.getOp( 1 ) )
        result = self.engine.execute( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result )
        self.assertEqual( test_result, result_data[0:len(test_result)] )

    def test04_value_retreval(self):
        test_result = 59.765625
        task_args = self.getTaskArgs( op=self.getOp( 2 ) )
        result = self.engine.execute( TaskRequest( request=task_args ) )
        result_data = self.getResultData( result )
        self.assertEqual( test_result, result_data )

    def test05_multitask(self):
        test_results = [ [ -1.405364990234375, -1.258880615234375, 0.840728759765625 ], [48.07984754774306, 49.218166775173614, 49.36114501953125], 59.765625 ]
        task_args = self.getTaskArgs( op=self.operations )
        results = self.engine.execute( TaskRequest( request=task_args ) )
        for ir in range(len(results)):
            rdata = self.getResultData( results, ir )
            test_result = test_results[ir]
            if hasattr( test_result, '__iter__' ):  self.assertEqual( test_result, rdata[0:len(test_result)] )
            else:                                   self.assertEqual( test_result, rdata )

if __name__ == '__main__':
    test_runner = unittest.TextTestRunner(verbosity=2)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase( EngineTests )
    test_runner.run( suite )

