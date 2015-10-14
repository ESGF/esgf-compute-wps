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

    def test01(self):
        result_data = [   -1.405364990234375, -1.258880615234375, 0.840728759765625, 2.891510009765625,  -18.592864990234375, -11.854583740234375, -3.212005615234375, -5.311614990234375, 5.332916259765625, -1.698333740234375, 8.750885009765625 ]
        request_parms = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
        request_parms['datainputs'] = [u'[domain=[{"id":"upper","level":{"start":0,"end":1,"system":"indices"}},{"id":"lower","level":{"start":2,"end":3,"system":"indices"}}];variable={"dset":"MERRA/mon/atmos/hur","id":"v0:hur","domain":"upper"};operation=["CDTime.departures(v0,slice:t)","CDTime.climatology(v0,slice:t,bounds:annualcycle)"];]']
        response_json = taskManager.processRequest( request_parms )
        responses = json.loads(response_json)
        for iR, result in enumerate(responses):
            test_result = result_data[iR]
            response_data = result['data']
            self.assertEqual( test_result, response_data[0:len(test_result)] )
