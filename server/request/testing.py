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
        result_data = [ [ 0.33635711669921875, -0.5425491333007812, -0.7378616333007812, -3.2769241333007812, 0.43401336669921875 ], [76.47098795572917, 75.69271511501736, 76.06928168402777, 75.85688612196181, 77.02577718098958, 77.63056098090277] ]
        request_parms = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
        request_parms['datainputs'] = [u'[region={"longitude":-44.17499999999998,"latitude":28.0645809173584,"level":100000,"time":"2010-01-16T12:00:00"};data={ "MERRA/mon/atmos": [ "v0:hur" ] };operation=["time.departures(v0,t)","time.climatology(v0,t,annualcycle)"];]']
        response_json = taskManager.processRequest( request_parms )
        responses = json.loads(response_json)
        for iR, result in enumerate(responses):
            test_result = result_data[iR]
            response_data = result['data']
            self.assertEqual( test_result, response_data[0:len(test_result)] )
