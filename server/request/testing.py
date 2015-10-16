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

    def xtest01(self):
        result_data = [   -1.405364990234375, -1.258880615234375, 0.840728759765625, 2.891510009765625,  -18.592864990234375, -11.854583740234375, -3.212005615234375, -5.311614990234375, 5.332916259765625, -1.698333740234375, 8.750885009765625 ]
        request_parms = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
        request_parms['datainputs'] = [u'[domain=[{"id":"upper","level":{"start":0,"end":1,"system":"indices"},"longitude":-44.1,"latitude":28.0,"time":"2010-01-16T12:00:00"},{"id":"lower","level":{"start":2,"end":3,"system":"indices"},"longitude":-44.1,"latitude":28.0,"time":"2010-01-16T12:00:00"}];variable={"dset":"MERRA/mon/atmos","id":"v0:hur","domain":"upper"};operation=["CDTime.departures(v0,slice:t)","CDTime.climatology(v0,slice:t,bounds:annualcycle)"];]']
        response_json = taskManager.processRequest( request_parms )
        responses = json.loads(response_json)
        for iR, result in enumerate(responses):
            response_data = result['data']
            self.assertEqual( result_data, response_data[0:len(result_data)] )

    def xtest02(self):
        request_parms = {'version': [u'1.0.0'], 'service': [u'WPS'], 'async': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'] }
        request_parms['datainputs'] = [u'[domain=[{"id":"upper","level":{"start":0,"end":1,"system":"indices"}},{"id":"lower","level":{"start":2,"end":3,"system":"indices"}}];variable={"dset":"MERRA/mon/atmos","id":"v0:hur","domain":"upper"};operation=["CDTime.departures(v0,slice:t)"];]']
        response_json = taskManager.processRequest( request_parms )
        response = json.loads(response_json)
        print response
