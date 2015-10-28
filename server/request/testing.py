import unittest, sys, logging, json
from request.manager import taskManager
from modules.utilities import wpsLog
verbose = False

if verbose:
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)

class KernelTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test01_average(self):
        test_result = [ 78.51173231823779, 78.84015402080719, 78.29642396331322, 77.87053204967265, 77.86940510900665, 78.79289460932043, 79.00662438505067, 78.73974144579576, 78.20356569815867, 78.31290437885096 ]
        request_parms = {'version': ['1.0.0'], 'service': ['WPS'], 'embedded': ['true'], 'async': ['false'], 'rawDataOutput': ['result'], 'identifier': ['cdas'], 'request': ['Execute'] }
        request_parms['datainputs'] = ['[domain=[{"id":"d0","level":{"start":0,"end":1,"system":"indices"}}];variable={"dset":"MERRA/mon/atmos","id":"v0:hur","domain":"d0"};operation=["CWT.average(v0,axis:xy)"];]']
        response_json = taskManager.processRequest( request_parms )
        response = json.loads(response_json)
        response_data = response['data']
        self.assertEqual( test_result, response_data[0:len(test_result)] )

    def test02_average(self):
        test_result = [ 78.51173231823779, 78.84015402080719, 78.29642396331322, 77.87053204967265, 77.86940510900665, 78.79289460932043, 79.00662438505067, 78.73974144579576, 78.20356569815867, 78.31290437885096 ]
        request_parms = {'version': ['1.0.0'], 'service': ['WPS'], 'embedded': ['true'], 'async': ['false'], 'rawDataOutput': ['result'], 'identifier': ['cdas'], 'request': ['Execute'] }
        request_parms['datainputs'] = ['[domain=[{"id":"d0","level":{"start":0,"end":1,"system":"indices"}}];variable={"dset":"MERRA/mon/atmos","id":"v0:hur","domain":"d0"};operation=["CWT.average(v0,axis:xy)"];]']
        response_json = taskManager.processRequest( request_parms )
        response = json.loads(response_json)
        response_data = response['data']
        self.assertEqual( test_result, response_data[0:len(test_result)] )

    def test03_ensemble_average(self):
        test_result = [ 287.3746163535607, 287.32201028468666, 288.0899054125938, 288.8933610641959, 289.69324597969796, 290.6019424186093, 290.5856389458788, 290.3434735042316, 290.11871301777, 289.1724024490968 ]
        request_parms = {'version': ['1.0.0'], 'service': ['WPS'], 'embedded': ['true'], 'async': ['false'], 'rawDataOutput': ['result'], 'identifier': ['cdas'], 'request': ['Execute'] }
        request_parms['datainputs'] = ['[domain=[{"id":"d0","level":{"start":0,"end":1,"system":"indices"}}];variable=[{"dset":"MERRA/mon/atmos","id":"v0:ta","domain":"d0"},{"dset":"CFSR/mon/atmos","id":"v0:ta","domain":"d0"}];operation=["CWT.average(*,axis:exy)"];]']
        response_json = taskManager.processRequest( request_parms )
        response = json.loads(response_json)
        response_data = response['data']
        self.assertEqual( test_result, response_data[0:len(test_result)] )

    def test04_ensemble_average(self):
        test_result = [ 287.3746163535607, 287.32201028468666, 288.0899054125938, 288.8933610641959, 289.69324597969796, 290.6019424186093, 290.5856389458788, 290.3434735042316, 290.11871301777, 289.1724024490968 ]
        request_parms = {'version': ['1.0.0'], 'service': ['WPS'], 'embedded': ['true'], 'async': ['false'], 'rawDataOutput': ['result'], 'identifier': ['cdas'], 'request': ['Execute'] }
        request_parms['datainputs'] = ['[domain=[{"id":"d0","level":{"start":0,"end":1,"system":"indices"}}];variable=[{"dset":"MERRA/mon/atmos","id":"v0:ta","domain":"d0"},{"dset":"CFSR/mon/atmos","id":"v0:ta","domain":"d0"}];operation=["CWT.average(*,axis:exy)"];]']
        response_json = taskManager.processRequest( request_parms )
        response = json.loads(response_json)
        response_data = response['data']
        self.assertEqual( test_result, response_data[0:len(test_result)] )

if __name__ == '__main__':
    test_runner = unittest.TextTestRunner(verbosity=2)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase( KernelTests )
    test_runner.run( suite )

