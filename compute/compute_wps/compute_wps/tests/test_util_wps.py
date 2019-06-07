from django import test

from compute_wps import models
from compute_wps.util import wps_response

class TestUtilWPSTestCase(test.TestCase):
    fixtures = ['processes.json', 'users.json']

    def test_describe_process(self):
        processes = models.Process.objects.filter(identifier='CDAT.subset') 

        data = wps_response.describe_process(processes)

        self.assertIn('wps:ProcessDescriptions', data)
        self.assertIn('CDAT.subset', data)
        self.assertIn('variable', data)
        self.assertIn('domain', data)
        self.assertIn('operation', data)
    
    def test_get_capabilities(self):
        processes = models.Process.objects.filter(identifier='CDAT.subset') 

        data = wps_response.get_capabilities(processes)

        self.assertIn('wps:Capabilities', data)
        self.assertIn('CDAT.subset', data)

    def test_exception_report(self):
        data = wps_response.exception_report(wps_response.NoApplicableCode, 'file access denied')

        self.assertIn('ows:ExceptionReport', data)
        self.assertIn('file access denied', data) 
