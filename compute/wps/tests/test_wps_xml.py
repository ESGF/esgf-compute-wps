
from cwt import wps_lib
from cwt.wps_lib import metadata
from cwt.wps_lib import operations
from django import test

from wps import models
from wps import wps_xml

class WPSXMLTestCase(test.TestCase):

    def test_create_identification(self):
        identification = wps_xml.create_identification()

        self.assertIsInstance(identification, metadata.ServiceIdentification)

    def test_create_provider(self):
        provider = wps_xml.create_provider()

        self.assertIsInstance(provider, metadata.ServiceProvider)

    def test_create_languages(self):
        languages = wps_xml.create_languages()

        self.assertIsInstance(languages, metadata.Languages)

    def test_create_operations(self):
        operations = wps_xml.create_operations()

        self.assertIsInstance(operations, list)
        self.assertEqual(len(operations), 3)

    def test_capabilities_response(self):
        process = models.Process.objects.create(identifier='CDAT.test', backend='Local', abstract='Abstract Data')

        data = wps_xml.capabilities_response([process])

        self.assertIsInstance(data, operations.GetCapabilitiesResponse)

        xml = data.xml()

        self.assertIn('CDAT.test', xml)
        self.assertIn('Abstract Data', xml)

    def test_describe_process_response(self):
        data = wps_xml.describe_process_response('CDAT.test', 'CDAT Subset', 'Abstract Data')

        self.assertIsInstance(data, operations.DescribeProcessResponse)

        xml = data.xml()

        self.assertIn('CDAT.test', xml)
        self.assertIn('CDAT Subset', xml)
        self.assertIn('Abstract Data', xml)

    def test_execute_response(self):
        data = wps_xml.execute_response('status_location', wps_lib.ProcessAccepted(), 'CDAT.test')

        self.assertIsInstance(data, operations.ExecuteResponse)

        xml = data.xml()

        self.assertIn('status_location', xml)
        self.assertIn('CDAT.test', xml)

    def test_load_output(self):
        input_data = metadata.Output(identifier='output', title='Output', data=metadata.ComplexData(value='test_data'))

        data = wps_xml.load_output(input_data.xml())

        self.assertIsInstance(data, metadata.Output)
    
    def test_create_output(self):
        data = wps_xml.create_output('test_data')

        self.assertIn('test_data', data)
